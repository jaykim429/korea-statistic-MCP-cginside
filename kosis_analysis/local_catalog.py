"""로컬 KOSIS 표 목록 검색(앱 사이클 8) — 글자 2-gram BM25.

KOSIS 통합검색은 질문 낱말과 표 제목 낱말이 다르면 정답 표를 상위 40 밖으로 냈다(앱 사이클 4·7). 통계목록 API 로 모은
전체 표(제목 + 분류 경로)를 여기서 다시 찾는다. 한국어 띄어쓰기·복합어·어순 차이를 형태소 분석이나 사전 없이 흡수하려고
글자 2-gram 을 쓴다. 분류 경로(주제 > 조사)는 문맥이다 — 제목에 없는 조사 낱말(「가축동향조사」 의 가축)을 더한다.

목록 파일이 없으면 검색하지 않는다(`load_default_catalog()` 가 None) — 기능이 꺼진 채 지금과 같다.
"""
from __future__ import annotations

import gzip
import json
import math
import os
import re
from functools import lru_cache
from typing import Any, Iterable, Optional

import numpy as np
from scipy import sparse

K1 = 1.2
B = 0.75
_STRIP = re.compile(r"[\s·ㆍ・,.()\[\]{}<>/\\\-_~:;'\"“”‘’「」『』]+")
DEFAULT_PATH = os.path.join(os.path.dirname(__file__), "data", "kosis_catalog.jsonl.gz")


def char_bigrams(text: Any) -> list[str]:
    """공백·문장부호를 지운 뒤 겹치는 2 글자. 한 글자짜리는 그 글자 하나."""
    compact = _STRIP.sub("", str(text or "").lower())
    if len(compact) < 2:
        return [compact] if compact else []
    return [compact[i:i + 2] for i in range(len(compact) - 1)]


def word_bigrams(text: Any) -> list[str]:
    """낱말(공백·문장부호로 나눈 조각)마다 겹치는 2 글자 — 낱말 경계를 넘는 쌍은 만들지 않는다. 한 글자 낱말은 그 글자."""
    grams: list[str] = []
    for word in _STRIP.split(str(text or "").lower()):
        if len(word) == 1:
            grams.append(word)
        grams.extend(word[i:i + 2] for i in range(len(word) - 1))
    return grams


_GRAMS = {"cross": char_bigrams, "word": word_bigrams}


def document_text(row: dict[str, Any], with_path: bool = True) -> str:
    title = str(row.get("title") or "")
    return f"{title} {' '.join(str(p) for p in row.get('path') or [])}" if with_path else title


class LocalCatalog:
    """표 하나 = 문서 하나. 질의 점수는 BM25(K1, B)를 미리 계산한 문서 × 2-gram 가중치 행렬의 열 합이다."""

    def __init__(self, rows: Iterable[dict[str, Any]], with_path: bool = True, grams: str = "cross"):
        self._grams = _GRAMS[grams]
        self.rows = [row for row in rows if row.get("orgId") and row.get("tblId")]
        self._vocab: dict[str, int] = {}
        indptr, indices, counts = [0], [], []
        lengths = []
        for row in self.rows:
            doc_grams = self._grams(document_text(row, with_path))
            lengths.append(len(doc_grams))
            tf: dict[int, int] = {}
            for gram in doc_grams:
                col = self._vocab.setdefault(gram, len(self._vocab))
                tf[col] = tf.get(col, 0) + 1
            indices.extend(tf.keys())
            counts.extend(tf.values())
            indptr.append(len(indices))
        # 목록 어휘 — 표 제목·분류 경로의 낱말(2 글자 이상). 붙여 쓴 질문 낱말을 나누는 데 쓴다(segment_query).
        self.lexicon = {word for row in self.rows for word in _STRIP.split(document_text(row, True).lower()) if len(word) >= 2}
        self._longest_word = max((len(word) for word in self.lexicon), default=0)
        n_docs, n_terms = len(self.rows), len(self._vocab)
        if not n_docs or not n_terms:
            self._weights = None
            return
        tf_matrix = sparse.csr_matrix((np.asarray(counts, dtype=np.float32), indices, indptr), shape=(n_docs, n_terms))
        df = np.bincount(tf_matrix.indices, minlength=n_terms).astype(np.float32)
        idf = np.log(1.0 + (n_docs - df + 0.5) / (df + 0.5)).astype(np.float32)
        length = np.asarray(lengths, dtype=np.float32)
        norm = K1 * (1.0 - B + B * length / max(float(length.mean()), 1.0))
        coo = tf_matrix.tocoo()
        saturated = coo.data * (K1 + 1.0) / (coo.data + norm[coo.row])
        self._weights = sparse.csc_matrix((saturated * idf[coo.col], (coo.row, coo.col)), shape=(n_docs, n_terms))

    def search(self, query: Any, k: int = 20) -> list[tuple[dict[str, Any], float]]:
        """점수 > 0 인 상위 k 개 (row, score). 같은 점수는 목록 순서."""
        if self._weights is None or k <= 0:
            return []
        cols = sorted({self._vocab[g] for g in self._grams(query) if g in self._vocab})
        if not cols:
            return []
        scores = np.asarray(self._weights[:, cols].sum(axis=1)).ravel()
        positive = np.flatnonzero(scores > 0)
        if not positive.size:
            return []
        top = positive[np.lexsort((positive, -scores[positive]))][:k]
        return [(self.rows[i], float(scores[i])) for i in top]


    def segment_query(self, query: Any) -> str:
        """목록에 없는 질문 낱말만 목록 낱말로 나눈다 — 덮지 못한 글자 수가 가장 적고, 그다음 조각 수가 가장 적은 분할.

        목록에 있는 낱말(「중소기업」)과 두 글자 이하 낱말은 그대로 둔다. 덮지 못한 한 글자(조사 「은」 「의」)는 버리고,
        두 글자 이상 덮지 못한 부분은 낱말로 남긴다 — 모르는 내용어를 버려 관문이 느슨해지지 않게.
        """
        out: list[str] = []
        for token in re.findall(r"[0-9A-Za-z가-힣]+", str(query or "").lower()):
            out.extend(self._segment(token) if len(token) > 2 and token not in self.lexicon else [token])
        return " ".join(out)

    def _segment(self, token: str) -> list[str]:
        n = len(token)
        # best[i] = (덮지 못한 글자, 조각 수, 조각들) — token[:i] 의 가장 나은 분할
        best: list[tuple[int, int, list[tuple[int, int, bool]]]] = [(0, 0, [])] + [(n + 1, n + 1, [])] * n
        for i in range(1, n + 1):
            skip = best[i - 1]
            best[i] = (skip[0] + 1, skip[1], skip[2] + [(i - 1, i, False)])
            for j in range(max(0, i - self._longest_word), i - 1):
                if token[j:i] in self.lexicon:
                    cand = best[j]
                    if (cand[0], cand[1] + 1) < best[i][:2]:
                        best[i] = (cand[0], cand[1] + 1, cand[2] + [(j, i, True)])
        uncovered, pieces, spans = best[n]
        if not pieces:
            return [token]
        words: list[str] = []
        run = ""
        for start, end, known in spans + [(n, n, True)]:
            if not known:
                run += token[start:end]
                continue
            if len(run) >= 2:
                words.append(run)
            run = ""
            if end > start:
                words.append(token[start:end])
        return words


def read_catalog(path: str) -> list[dict[str, Any]]:
    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


@lru_cache(maxsize=1)
def load_default_catalog() -> Optional[LocalCatalog]:
    """환경 변수 `KOSIS_LOCAL_CATALOG`(경로) 또는 기본 파일. `KOSIS_LOCAL_SEARCH=off` 이거나 파일이 없으면 None."""
    if str(os.environ.get("KOSIS_LOCAL_SEARCH", "")).strip().lower() == "off":
        return None
    path = os.environ.get("KOSIS_LOCAL_CATALOG") or DEFAULT_PATH
    if not os.path.exists(path):
        return None
    with_path = str(os.environ.get("KOSIS_LOCAL_WITH_PATH", "on")).strip().lower() != "off"
    grams = str(os.environ.get("KOSIS_LOCAL_GRAMS", "cross")).strip().lower()
    return LocalCatalog(read_catalog(path), with_path=with_path, grams=grams if grams in _GRAMS else "cross")

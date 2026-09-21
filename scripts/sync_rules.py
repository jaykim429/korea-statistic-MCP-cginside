# -*- coding: utf-8 -*-
"""정본 규칙 JSON 을 이 저장소로 복사한다. **손으로 베끼지 않는다.**

왜 복사인가.
Dockerfile 이 ``COPY . .`` 라 컨테이너는 상위(Nuxt) 디렉터리를 볼 수 없다. 그래서 빌드 전에
정본을 이 저장소 안으로 가져온다. 정본은 여전히 Nuxt 쪽 ``server/utils/stat/rules/`` 한 곳이고
여기 ``rules/`` 는 사본이다.

손으로 베끼면 반드시 어긋난다 — 그렇게 해서 총계 라벨이 세 곳에 서로 다른 내용으로
존재하게 됐다(실측 2026-09-18).

쓰는 법 (저장소 루트에서)::

    python scripts/sync_rules.py            # 복사
    python scripts/sync_rules.py --check    # 같은지만 본다 (시험이 쓴다)
"""
from __future__ import annotations

import argparse
import filecmp
import shutil
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent.parent
SOURCE = HERE.parent / "server" / "utils" / "stat" / "rules"
DEST = HERE / "rules"
FILES = ("total-label.json", "item-label-cases.json")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="복사하지 않고 같은지만 본다")
    args = parser.parse_args()

    if not SOURCE.is_dir():
        print("정본을 찾을 수 없다: %s" % SOURCE)
        return 1
    DEST.mkdir(exist_ok=True)

    stale = []
    for name in FILES:
        src, dst = SOURCE / name, DEST / name
        if not src.is_file():
            print("정본에 없는 파일: %s" % src)
            return 1
        if args.check:
            if not dst.is_file() or not filecmp.cmp(src, dst, shallow=False):
                stale.append(name)
        else:
            shutil.copy2(src, dst)
            print("  복사 %s" % name)

    if args.check and stale:
        print("사본이 정본과 다르다: %s" % ", ".join(stale))
        print("고치려면: python scripts/sync_rules.py")
        return 1
    print("정본과 같다" if args.check else "완료")
    return 0


if __name__ == "__main__":
    sys.exit(main())

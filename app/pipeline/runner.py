from __future__ import annotations
import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any, Dict

PROJECT_ROOT = Path(__file__).resolve().parents[2]  # ~/cash-ai-01
ORIGINALS_DIR = PROJECT_ROOT / "app" / "pipeline" / "originals"

def _run(cmd: list[str], cwd: Path, env: Dict[str, str]) -> None:
    p = subprocess.run(
        cmd,
        cwd=str(cwd),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    if p.returncode != 0:
        raise RuntimeError(f"Command failed: {' '.join(cmd)}\n--- output ---\n{p.stdout}")

def run_001_002_003(payload: Dict[str, Any]) -> Dict[str, Any]:
    data_json = {
        "BS": payload.get("BS", []),
        "PL": payload.get("PL", []),
        "販売費": payload.get("SGA", []),
        "製造原価": payload.get("MFG", []),
    }

    run_dir = Path(tempfile.mkdtemp(prefix="cashai_", dir="/tmp"))
    (run_dir / "data.json").write_text(json.dumps(data_json, ensure_ascii=False), encoding="utf-8")

    api_key = os.environ.get("OPENAI_API_KEY", "")
    env = dict(os.environ)
    if api_key:
        env["OPENAI_API_KEY2"] = api_key

    # ★ここが重要：cash-ai-01 直下をPYTHONPATHに入れる（google/colabスタブを拾える）
    env["PYTHONPATH"] = str(PROJECT_ROOT)

    _run(["python3", str(ORIGINALS_DIR / "cloab001.py")], cwd=run_dir, env=env)
    _run(["python3", str(ORIGINALS_DIR / "cloab002.py")], cwd=run_dir, env=env)
    _run(["python3", str(ORIGINALS_DIR / "cloab003.py")], cwd=run_dir, env=env)

    out_path = run_dir / "output_updated.json"
    if not out_path.exists():
        out_path = run_dir / "output.json"
    if not out_path.exists():
        raise RuntimeError("output_updated.json / output.json が生成されませんでした。")

    return json.loads(out_path.read_text(encoding="utf-8"))


# ============================================================
# Cloud Run 用: colab1-1-checkByClaude.py 相当の処理（Anthropic）
# ============================================================
import base64
import re
from datetime import datetime, timezone
from typing import List, Optional, Tuple

import boto3

try:
    import anthropic  # type: ignore
except Exception:  # pragma: no cover
    anthropic = None  # 依存が無い場合にエラーメッセージを出すため


def _parse_s3_uri(uri: str) -> Tuple[str, str]:
    # s3://bucket/key
    if not uri.startswith("s3://"):
        raise ValueError(f"Unsupported uri (expected s3://...): {uri}")
    no_scheme = uri[len("s3://") :]
    parts = no_scheme.split("/", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ValueError(f"Invalid s3 uri: {uri}")
    return parts[0], parts[1]


def _s3_client():
    # runner101.py と同じ環境変数名に合わせる
    access_key = os.getenv("S3_ACCESS_KEY")
    secret_key = os.getenv("S3_SECRET_KEY")
    region = os.getenv("S3_REGION")
    if not access_key or not secret_key or not region:
        raise RuntimeError("S3_ACCESS_KEY / S3_SECRET_KEY / S3_REGION が未設定です。")
    return boto3.client(
        "s3",
        region_name=region,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
    )


def _download_s3_to_tmp(s3_uri: str, run_dir: Path, index: int | None = None) -> Path:
    bucket, key = _parse_s3_uri(s3_uri)
    base = Path(key).name or "input.pdf"
    # 1リクエスト内で同名ファイルが複数あるケースや、意図せず上書きしないようにユニーク化
    stem = Path(base).stem
    suffix = Path(base).suffix or ".pdf"
    if index is not None:
        candidate = f"{stem}_{index}{suffix}"
    else:
        candidate = base
    local = run_dir / candidate
    n = 1
    while local.exists():
        local = run_dir / f"{stem}_{index or 0}_{n}{suffix}"
        n += 1
    s3 = _s3_client()
    s3.download_file(bucket, key, str(local))
    return local



def _split_pdfurls(pdfurls: Any) -> List[str]:
    if not pdfurls:
        return []
    if isinstance(pdfurls, list):
        return [str(x).strip() for x in pdfurls if str(x).strip()]
    s = str(pdfurls).strip()
    if not s:
        return []
    # 例: "s3://...pdf|,|s3://...pdf"
    parts = [p.strip() for p in s.split("|,|")]
    return [p for p in parts if p]


def _anthropic_client():
    api_key = os.getenv("ANTHROPIC_API_KEY") or os.getenv("CLAUDE_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY（または CLAUDE_API_KEY）が未設定です。")
    if anthropic is None:
        raise RuntimeError("anthropic ライブラリがインストールされていません。requirements.txt を確認してください。")
    return anthropic.Anthropic(api_key=api_key)


def _to_int(v: Any) -> int:
    if v is None or v == "":
        return 0
    try:
        return int(str(v).replace(",", "").strip())
    except Exception:
        return 0


def _find_amount(rows: list, name: str, period: str) -> Optional[int]:
    for row in rows or []:
        if isinstance(row, dict) and row.get("勘定科目") == name:
            p = row.get(period) or {}
            if isinstance(p, dict):
                return _to_int(p.get("金額"))
    return None


def _agent2_numeric_checks(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Colabの Agent2 を Cloud Run で再現：JSONの数値だけで機械的に検算する"""
    bs = payload.get("BS") or []
    pl = payload.get("PL") or payload.get("pl") or []
    periods = ["今期", "前期", "前々期"]

    checks = []

    # BS: 資産合計 == 負債純資産合計
    for per in periods:
        assets = _find_amount(bs, "資産合計", per) or _find_amount(bs, "資産合計", per)
        liab_eq = _find_amount(bs, "負債純資産合計", per) or _find_amount(bs, "負債及び純資産合計", per)
        if assets is not None and liab_eq is not None:
            diff = assets - liab_eq
            checks.append(
                {
                    "type": "BS_balance",
                    "period": per,
                    "left": {"name": "資産合計", "amount": assets},
                    "right": {"name": "負債純資産合計", "amount": liab_eq},
                    "diff": diff,
                    "ok": diff == 0,
                }
            )

    # PL: 代表的な利益の式（存在する項目だけ）
    # 売上総利益 = 売上高 - 売上原価
    for per in periods:
        sales = _find_amount(pl, "売上高", per)
        cogs = _find_amount(pl, "売上原価", per)
        gp = _find_amount(pl, "売上総利益", per)
        if sales is not None and cogs is not None and gp is not None:
            diff = gp - (sales - cogs)
            checks.append(
                {
                    "type": "PL_gross_profit",
                    "period": per,
                    "expected": sales - cogs,
                    "actual": gp,
                    "diff": diff,
                    "ok": diff == 0,
                }
            )

    return {
        "periods": periods,
        "checks": checks,
        "summary": {
            "total": len(checks),
            "ok": sum(1 for c in checks if c.get("ok")),
            "ng": sum(1 for c in checks if not c.get("ok")),
        },
    }


def run_check_by_claude(payload: Dict[str, Any]) -> Dict[str, Any]:
    """API から呼ばれるエントリポイント。
    入力payload（BS/PL と pdfurls）を受け、PDF品質チェック + 数値検算 + 最終判定を返す。
    """
    if payload.get("nodoai") is True:
        return {
            "ai_case_id": payload.get("ai_case_id"),
            "postingPeriod": payload.get("postingPeriod"),
            "skipped": True,
            "reason": "nodoai=true のため AI チェックをスキップしました。",
            "created_at": datetime.now(timezone.utc).isoformat(),
        }

    run_dir = Path(tempfile.mkdtemp(prefix="checkbyclaude_", dir="/tmp"))

    # PDF 取得（S3）
    pdf_uris = _split_pdfurls(payload.get("pdfurls"))
    local_pdfs: List[Path] = []
    for uri in pdf_uris:
        if uri.startswith("s3://"):
            local_pdfs.append(_download_s3_to_tmp(uri, run_dir, index=len(local_pdfs)+1))
        else:
            # http(s) 等は現状未対応（必要なら curl/wget を追加）
            raise ValueError(f"Unsupported pdf url scheme: {uri}")

    # Agent2（数値検算）
    agent2 = _agent2_numeric_checks(payload)

    # Agent1 / Agent3（Claude）
    client = _anthropic_client()
    model = os.getenv("CLAUDE_MODEL") or "claude-3-5-sonnet-latest"

    # PDF を base64 へ
    docs = []
    for p in local_pdfs:
        b64 = base64.standard_b64encode(p.read_bytes()).decode("utf-8")
        docs.append(
            {
                "type": "document",
                "source": {"type": "base64", "media_type": "application/pdf", "data": b64},
            }
        )

    agent1_prompt = """あなたはPDF読取品質の検査員です。
添付された決算書PDFを見て、以下のみをチェックしてください。

## チェック対象（読取品質のみ）
- PDF画像の鮮明度・傾き・ノイズ
- 文字の潰れ・かすれ・影による読みにくさ
- 墨消し（黒塗り）の範囲と読取への影響
- スキャン品質として問題のある箇所

## 出力
次のJSONのみを返してください（説明文は不要）:
{
  "overall": "OK|WARN|NG",
  "issues": [{"page_hint": "string", "severity": "low|mid|high", "detail": "string"}],
  "notes": "string"
}
"""

    msg1 = client.messages.create(
        model=model,
        max_tokens=1200,
        temperature=0,
        messages=[
            {
                "role": "user",
                "content": [{"type": "text", "text": agent1_prompt}, *docs],
            }
        ],
    )
    agent1_raw = getattr(msg1, "content", None)
    # anthropic sdk は content が list で text が入ることが多い
    agent1_text = ""
    try:
        if isinstance(agent1_raw, list):
            agent1_text = "".join([c.get("text", "") for c in agent1_raw if isinstance(c, dict)])
        else:
            agent1_text = str(agent1_raw)
    except Exception:
        agent1_text = str(agent1_raw)

    def _parse_json_from_text(t: str) -> Dict[str, Any]:
        # ```json ...``` 優先。無ければ最初の {..} を拾う
        m = re.search(r"```json\s*(\{.*?\})\s*```", t, re.DOTALL)
        if m:
            return json.loads(m.group(1))
        m2 = re.search(r"(\{.*\})", t, re.DOTALL)
        if m2:
            return json.loads(m2.group(1))
        raise ValueError(f"Claude からJSONを抽出できませんでした: {t[:300]}")

    agent1 = _parse_json_from_text(agent1_text)

    agent3_prompt = """あなたは最終判定レビュアーです。
以下の2つの結果を読み、最終的な判定をJSONで返してください。

- agent1: PDF読取品質チェック（画像品質・墨消し影響など）
- agent2: 数値検算（機械的計算）

## 出力JSON
{
  "verdict": "PASS|WARN|FAIL",
  "reasons": ["string", ...],
  "recommendations": ["string", ...]
}
"""

    msg3 = client.messages.create(
        model=model,
        max_tokens=900,
        temperature=0,
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": agent3_prompt},
                    {"type": "text", "text": "agent1=" + json.dumps(agent1, ensure_ascii=False)},
                    {"type": "text", "text": "agent2=" + json.dumps(agent2, ensure_ascii=False)},
                ],
            }
        ],
    )

    agent3_raw = getattr(msg3, "content", None)
    agent3_text = ""
    try:
        if isinstance(agent3_raw, list):
            agent3_text = "".join([c.get("text", "") for c in agent3_raw if isinstance(c, dict)])
        else:
            agent3_text = str(agent3_raw)
    except Exception:
        agent3_text = str(agent3_raw)

    agent3 = _parse_json_from_text(agent3_text)

    return {
        "ai_case_id": payload.get("ai_case_id"),
        "postingPeriod": payload.get("postingPeriod"),
        "csvdownloadfilename": payload.get("csvdownloadfilename"),
        "model": model,
        "inputs": {
            "pdfurls": pdf_uris,
            "has_BS": bool(payload.get("BS")),
            "has_PL": bool(payload.get("PL") or payload.get("pl")),
        },
        "agent1": agent1,
        "agent2": agent2,
        "agent3": agent3,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


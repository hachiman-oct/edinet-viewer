"""
EDINET XBRL Batch Processor — CLI スタンドアロンスクリプト

GitHub Actions から直接実行し、BigQuery の documents テーブルから
未処理の有価証券報告書を取得 → XBRL パース → financial_summary /
segment_data / company_master にバルク投入する。

Usage:
    python batch_process.py --limit 100 --order desc
    python batch_process.py --limit 200 --order asc --credentials-json '{"type":"service_account",...}'
"""

import argparse
import logging
import os
import sys

from dotenv import load_dotenv
from google.cloud import bigquery

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from bq_service import (
    BQ_COMPANY_MASTER,
    BQ_DATASET,
    BQ_DOCUMENTS,
    BQ_FINANCIAL_SUMMARY,
    BQ_SEGMENT_DATA,
    FLUSH_CHUNK_SIZE,
    create_bq_client,
    execute_batch,
    insert_rows_load_job,
    mark_document_processed,
    upsert_company_master,
)
from xbrl_parser import (
    EDINET_API_DOC_URL,
    MAX_RETRIES,
    RETRY_BACKOFF_BASE,
    RETRYABLE_STATUS_CODES,
    download_xbrl_with_retry,
    extract_xbrl_data,
    parse_num,
)

# ── ログ設定 ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


def run_batch(client: bigquery.Client, api_key: str, limit: int, order: str):
    """未処理の documents を取得し、XBRL パース → BigQuery にバルク投入する。"""
    return execute_batch(client, api_key=api_key, limit=limit, order=order)


def main():
    parser = argparse.ArgumentParser(description="EDINET XBRL Batch Processor")
    parser.add_argument(
        "--limit", type=int, default=100, help="Max documents to process per run"
    )
    parser.add_argument(
        "--order",
        choices=["asc", "desc"],
        default="desc",
        help="Processing order: 'desc' (newest first) or 'asc' (oldest first)",
    )
    parser.add_argument(
        "--credentials-json",
        type=str,
        default=None,
        help="GCP service account JSON string (for CI/CD)",
    )
    args = parser.parse_args()

    # .env を読み込み（ローカル実行時）
    env_path = os.path.join(os.path.dirname(__file__), ".env")
    if os.path.exists(env_path):
        load_dotenv(env_path)

    api_key = os.environ.get("EDINET_API_KEY", "")
    if not api_key:
        logger.error(
            "EDINET_API_KEY is not set. Set it in .env or as an environment variable."
        )
        sys.exit(1)

    client = create_bq_client(args.credentials_json)
    logger.info(
        "🚀 Starting batch process (limit=%d, order=%s, project=%s)",
        args.limit,
        args.order,
        client.project,
    )

    run_batch(client, api_key, args.limit, args.order)


if __name__ == "__main__":
    main()

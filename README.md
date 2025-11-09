# Data-ETL-Pipeline-Assistant-Using-Agentic-RAG
An AI-powered assistant that uses Retrieval-Augmented Generation (RAG) and agentic reasoning to query databases, perform ETL on tabular data, and generate visual reports. Built with FastAPI, Streamlit, LangChain, ChromaDB, and Azure OpenAI.

## Local Setup Notes

- Install dependencies with `pip install -r requirements.txt`.
- Run Redis locally (e.g. `redis-server` or `docker run -p 6379:6379 redis`) so the agent cache works, and set `CACHE_REDIS_URL` / `CACHE_TTL_SECONDS` in `.env`.
- Start the FastAPI service with `uvicorn app.api.main:app --reload`.
- Build the retrieval corpus via `python -m app.index.build_corpus`.
- Trigger the end-to-end ETL pipeline through the API or run `python -m app.etl.json_to_s3 --all` (use `--disable-s3` to skip S3 uploads during local testing).
- Configure ETL sources/targets via `config/etl_manifest.json`. When present, the manifest overrides the `ETL_*` environment values (raw directory, file pattern, schema catalog, S3/DB options) so you can swap datasets by editing a single file.
- Set `transform.auto_mapping` to `true` in the manifest to let the agent call the schema-mapper. Few-shot column hints live under `transform.source_columns` and are cached in `.cache/etl_cache.json`.
- Use `"inherit"` (or omit the field) for manifest flags such as `enable_s3` when you want to fall back to environment configuration; set an explicit boolean only when you need to override.
- Duplicate-load recovery decisions are remembered in `.cache/etl_repair_knowledge.json`; clear the file if you want to reset the agent’s preferred strategy.
- Duplicate primary-key errors are remembered in Redis; the agent will skip those tables on subsequent runs and note the skip in the response.
- Launch the Streamlit UI with `streamlit run app/ui/streamlit-hello.py` to submit prompts, review retrieved context, explore results with on-the-fly charts, and download CSV/Excel exports without hard-coding schema knowledge.
- The assistant maintains conversation state per `session_id` (stored in Redis when available). The Streamlit UI passes this identifier automatically so follow-up prompts reuse previous context and intent.

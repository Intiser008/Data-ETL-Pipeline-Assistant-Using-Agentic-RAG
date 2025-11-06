# Data-ETL-Pipeline-Assistant-Using-Agentic-RAG
An AI-powered assistant that uses Retrieval-Augmented Generation (RAG) and agentic reasoning to query databases, perform ETL on tabular data, and generate visual reports. Built with FastAPI, Streamlit, LangChain, ChromaDB, and Azure OpenAI.

## Local Setup Notes

- Install dependencies with `pip install -r requirements.txt`.
- Run Redis locally (e.g. `redis-server` or `docker run -p 6379:6379 redis`) so the agent cache works, and set `CACHE_REDIS_URL` / `CACHE_TTL_SECONDS` in `.env`.
- Start the FastAPI service with `uvicorn app.api.main:app --reload`.
- Build the retrieval corpus via `python -m app.index.build_corpus`.
- Trigger the end-to-end ETL pipeline through the API or run `python -m app.etl.json_to_s3 --all` (use `--disable-s3` to skip S3 uploads during local testing).
- Duplicate primary-key errors are remembered in Redis; the agent will skip those tables on subsequent runs and note the skip in the response.

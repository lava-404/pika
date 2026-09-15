from fastapi import FastAPI, UploadFile, File
import pymupdf
import uuid
import os
from dotenv import load_dotenv
from langchain_text_splitters import RecursiveCharacterTextSplitter
from supabase import create_client
from sentence_transformers import SentenceTransformer

app = FastAPI(title="Nyx")

sources = {}

load_dotenv()

supabase = create_client(
    os.getenv("DATABASE_URL"),
    os.getenv("DATABASE_KEY")
)


@app.get("/")
def health():
    return {"status": "ok"}


@app.post("/upload")
async def upload(
    project_id: str,
    file: UploadFile = File(...)
):
    sources[file.filename] = {
        "content_type": file.content_type,
        "size": file.size
    }

    # Generate UUID for the source
    source_id = str(uuid.uuid4())

    source_result = supabase.table("sources").insert({
        "id": source_id,
        "project_id": project_id,
        "filename": file.filename,
        "type": "pdf",
        "storage_path": file.filename,
        "status": "parsing"
    }).execute()

    # Read uploaded PDF
    file_bytes = await file.read()

    # Open PDF directly from memory
    doc = pymupdf.open(
        stream=file_bytes,
        filetype="pdf"
    )

    # Extract text
    out = open("output.txt", "wb")

    for page in doc:
        text = page.get_text().encode("utf8")
        out.write(text)
        out.write(bytes((12,)))

    out.close()
    doc.close()

    # Read extracted text
    with open("output.txt", "r") as output_file:
      document = output_file.read()

    # Split text into chunks
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=100,
        chunk_overlap=0
    )

    texts = text_splitter.split_text(document)

    # Load embedding model once
    model = SentenceTransformer("all-MiniLM-L6-v2")

    # Generate and store chunks
    for i, text in enumerate(texts):

        # Generate UUID for chunk
        chunk_id = str(uuid.uuid4())

        # Generate embedding
        embedding = model.encode(text).tolist()

        chunks = supabase.table("chunks").insert({
            "id": chunk_id,
            "source_id": source_id,
            "chunk_index": i,
            "text": text,
            "embedding": embedding
        }).execute()

    return sources[file.filename]
# BNVT Dealflow - Azava Integration Layer

This directory contains the integration layer connecting BNVT's Dealflow Pipeline application to the Azava inbound intake automation platform.

---

## 1. Deployment & Setup

### Prerequisites
Make sure you have the following installed:
* Python 3.9+
* Python dependencies: `playwright`, `pypdf`, `pillow`, `requests` (can be auto-installed by scripts)
* Playwright browser binaries: Run `playwright install chromium` if not already installed.

### Start the Server
Run the standard pipeline server (defaults to port `8000`):
```bash
python3 server.py
```

---

## 2. Configuration & Secrets

### A. Azava Auth Secret
All Azava integration endpoints are protected using a Bearer token.
* **Storage Location**: `db_storage/azava_secret.json`
* **Default Secret**: `azava_super_secret_token_2026`
* **How to Configure**: Customize the token value directly in the file:
  ```json
  {
    "secret": "your_custom_secure_bearer_token_here"
  }
  ```
* **Usage**: Incoming requests must provide the custom header:
  `Authorization: Bearer <secret>`
  For GET requests (like `/pipeline/summary`), you can also pass it as a query parameter:
  `?secret=<secret>` or `?token=<secret>`

### B. OpenAI API Key (Whisper Transcription)
Voice note transcription utilizes OpenAI's Whisper API.
* **How to Configure**: Either:
  1. Save your API key in `db_storage/openai_api_key.json`:
     ```json
     {
       "value": "sk-proj-..."
     }
     ```
  2. Or export it in your environment before running the server:
     ```bash
     export OPENAI_API_KEY="sk-proj-..."
     ```

### C. Claude API Key (Diligence / Context Extraction)
Ensure the Anthropic API key is configured either in the client UI header, in `db_storage/anthropic_api_key.json`, or exported in your environment:
```bash
export ANTHROPIC_API_KEY="sk-ant-..."
```

---

## 3. API Endpoints

### 1. Remote Adapter endpoint (`POST /api/azava`)
Implements the Azava Remote Adapter Protocol.
* **Payload**: `{ "method": "<method>", "params": { ... } }`
* **Supported Methods**:
  * `manifest`: Returns adapter configuration metadata.
  * `listEntryPoints`: Exposes the `Deal` entry point type.
  * `describe`: Details the 10 core fields matching BNVT's database schema (`name`, `stage`, `source`, `notes`, `contactName`, `contactPhone`, `deckUrl`, `amount`, `sector`, `createdAt`).
  * `resolveEntity`: Performs fuzzy matching on company name and contact phone number.
  * `getFieldValue`: Returns individual property values (concatenates notes list into a plain text block).
  * `createRecord`: Inserts a new deal.
  * `updateRecord`: Edits an existing deal by ID. Returns `{ "notFound": true }` within HTTP 200 if the ID does not exist.
  * `deleteRecord`: Deletes a deal by ID.

### 2. Voice-note Intake (`POST /ingest/voice`)
Transcribes and extracts inbound deal info from a voice note.
* **Request (JSON)**:
  ```json
  {
    "url": "https://example.com/voice_note.mp3",
    "senderPhone": "+15550199",
    "senderName": "Jane Doe"
  }
  ```
  *(Alternatively, accepts a raw binary audio file payload with appropriate audio Content-Type)*
* **Response**: Returns the extracted metadata, transcript, and matched deal record details.

### 3. Deck Intake (`POST /ingest/deck`)
Downloads and processes direct pitch deck PDFs or DocSend links.
* **Request (JSON)**:
  ```json
  {
    "url": "https://docsend.com/view/abcd1234",
    "email": "visitor@example.com",
    "code": "optional_access_code"
  }
  ```
* **Response**: Returns the extracted company metadata, a one-paragraph AI summary, and the local PDF URL.

### 4. Pipeline Summary (`GET /pipeline/summary`)
Returns a concise summary of active deal flow, counts by stage, and the 5 most recent records.
* **Format**: Returns plain text by default, or structured JSON if `Accept: application/json` is sent or `?format=json` query param is present.

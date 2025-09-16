# Bluejay AI

An intelligent chat application that provides AI-driven insights and analysis for S&P 500 component stocks. The application features a modern React frontend with file upload capabilities and a FastAPI backend with OpenAI integration and TiDB database connectivity.

## Features

- 🤖 **AI-Powered Chat Interface**: Natural language queries about S&P 500 stocks
- 📁 **File Upload Support**: Upload documents, reports, and data files for analysis
- 💬 **Real-time Chat**: ChatGPT-like interface with message bubbles and typing indicators
- 🗄️ **Database Integration**: TiDB/MySQL database with secure TLS connections
- 🔐 **Secure Authentication**: Environment-based configuration for API keys
- 📊 **S&P 500 Data**: Pre-loaded stock data and analysis capabilities

## Project Structure

```
sp500_agentic_ai/
├── client/                 # React frontend application
│   ├── src/
│   │   ├── components/     # React components (Chat, Menu)
│   │   ├── assets/         # SVG icons and static assets
│   │   └── ...
│   ├── package.json
│   └── vite.config.ts
├── server/                 # FastAPI backend application
│   ├── main.py            # FastAPI application with chat endpoints
│   ├── db.py              # Database connection and queries
│   ├── config.py          # Environment configuration
│   ├── tools.py           # AI tools and utilities
│   ├── agent_core.py      # Core agent functionality
│   ├── memory.py          # Memory management
│   ├── ingest/            # Document processing pipeline
│   ├── requirements.txt   # Python dependencies
│   └── test/              # Test files
├── data/                  # S&P 500 stock data and uploads
│   ├── S_and_P_500_component_stocks.csv
│   ├── sp500_stooq_ohcl/  # Historical stock data
│   └── uploads/           # User uploaded files
├── data_fetching_scripts/ # Data collection and processing scripts
└── README.md
```

## Prerequisites

- **Node.js 18+** (for React frontend)
- **Python 3.8+** (for FastAPI backend)
- **TiDB/MySQL Database** (for data storage)
- **OpenAI API Key** (for AI functionality)

## Quick Start

### Option 1: Automated Setup (Recommended)

```bash
git clone https://github.com/hanumantjain/sp500_agentic_ai.git
cd sp500_agentic_ai
chmod +x setup.sh
./setup.sh
```

This script will:
- Set up the Python virtual environment
- Install all backend dependencies
- Install all frontend dependencies
- Create a `.env` template file

### Option 2: Manual Setup

### 1. Clone the Repository

```bash
git clone https://github.com/hanumantjain/sp500_agentic_ai.git
cd sp500_agentic_ai
```

### 2. Backend Setup (Server)

1. **Navigate to server directory:**
   ```bash
   cd server
   ```

2. **Create and activate virtual environment:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up environment variables:**
   ```bash
   cp config.template .env
   ```
   
   Edit `.env` with your configuration:
   ```env
   # OpenAI Configuration
   OPENAI_API_KEY=your_openai_api_key_here

   # TiDB/MySQL Database Configuration
   TIDB_HOST=your_tidb_host
   TIDB_PORT=4000
   TIDB_USER=your_username
   TIDB_PASSWORD=your_password
   TIDB_DB_NAME=your_database_name
   CA_PATH=/path/to/ca-cert.pem  # Optional: for TLS connections
   ```

5. **Start the FastAPI server:**
   ```bash
   uvicorn main:app --reload --port 8000
   ```

### 3. Frontend Setup (Client)

1. **Navigate to client directory:**
   ```bash
   cd ../client
   ```

2. **Install Node.js dependencies:**
   ```bash
   npm install
   ```

3. **Start the development server:**
   ```bash
   npm run dev
   ```

4. **Open your browser:**
   - Go to `http://localhost:5173`
   - You should see the chat interface

## API Endpoints

### Chat Endpoints

- **POST `/ask`** - Send a question with optional file attachments
  - Body: `FormData` with `question` (string) and `files` (File[])
  - Response: `{ "reply": "AI response", "sql": "generated SQL", "results": [...] }`

- **POST `/hello`** - Simple health check endpoint
  - Response: `{ "reply": "hello" }`

### File Upload

The application supports uploading various file types:
- Images: `.jpg`, `.png`, `.gif`
- Documents: `.pdf`, `.doc`, `.docx`, `.txt`
- Data files: `.csv`, `.xlsx`

Files are processed and their content is included in the AI analysis.

## Configuration

### Environment Variables

Create a `.env` file in the `server/` directory:

```env
# Required: OpenAI API Key
OPENAI_API_KEY=sk-your-openai-api-key

# Required: TiDB/MySQL Database
TIDB_HOST=your-database-host
TIDB_PORT=4000
TIDB_USER=your-username
TIDB_PASSWORD=your-password
TIDB_DB_NAME=your-database-name

# Optional: TLS Configuration
CA_PATH=/path/to/ca-cert.pem
```

### Database Setup

The application expects a TiDB/MySQL database with TLS support. For TiDB Cloud:

1. Create a TiDB Cloud account
2. Set up a serverless cluster
3. Enable TLS connections
4. Update your environment variables with the connection details

## Development

### Backend Development

```bash
cd server
source .venv/bin/activate
uvicorn main:app --reload --port 8000
```

### Frontend Development

```bash
cd client
npm run dev
```

### Testing

Test the backend API:
```bash
curl -X POST "http://localhost:8000/hello"
```

Test with file upload:
```bash
curl -X POST "http://localhost:8000/ask" \
  -F "question=What is the current price of AAPL?" \
  -F "files=@/path/to/your/file.pdf"
```

## Troubleshooting

### Common Issues

1. **CORS Errors**
   - The Vite dev server is configured to proxy `/ask` requests to the backend
   - Ensure the backend is running on port 8000

2. **Database Connection Issues**
   - Verify your TiDB credentials in `.env`
   - Ensure TLS is properly configured for TiDB Cloud
   - Check that the `CA_PATH` points to a valid certificate

3. **OpenAI API Errors**
   - Verify your OpenAI API key is valid and has sufficient credits
   - Check that the key has access to the GPT-4 models

4. **File Upload Issues**
   - Ensure files are within size limits
   - Check that file types are supported
   - Verify the backend can process the uploaded files

### Error Handling

The application includes graceful error handling:
- Missing API keys return mock responses instead of 500 errors
- Database connection issues are logged and handled gracefully
- File processing errors are caught and reported to the user

## Data Sources

The application includes:
- **S&P 500 Component Stocks**: Complete list of S&P 500 companies
- **Historical Stock Data**: OHLC data from Stooq
- **Symbol Mapping**: Mapping between different stock symbol formats
- **Upload Directory**: User-uploaded documents and reports

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues and questions:
1. Check the troubleshooting section above
2. Review the error logs in the browser console and server terminal
3. Ensure all environment variables are properly configured
4. Verify database connectivity and OpenAI API access

## Setup Script

The project includes an automated setup script (`setup.sh`) that handles the entire development environment setup:

```bash
./setup.sh
```

This script will:
- ✅ Verify Python 3.8+ and Node.js 18+ are installed
- ✅ Create and activate a Python virtual environment
- ✅ Install all Python dependencies from `requirements.txt`
- ✅ Install all Node.js dependencies via npm
- ✅ Create a `.env` file from the template
- ✅ Provide next steps for configuration

## Roadmap

- [ ] User authentication and session management
- [ ] Advanced charting and visualization
- [ ] Real-time stock data integration
- [ ] Portfolio tracking and analysis
- [ ] News sentiment analysis
- [ ] Advanced AI agent capabilities
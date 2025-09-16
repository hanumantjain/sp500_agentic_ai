# Contributing to Bluejay AI
=============================

Thanks for thinking about contributing to Bluejay AI — we appreciate your interest in helping improve our S&P 500 Agentic AI platform! This guide keeps contributions consistent and fast to review.

## Table of Contents
-----------------

- [Code of Conduct](#code-of-conduct)
- [Ways to Contribute](#ways-to-contribute)
- [Before You Start](#before-you-start)
- [Project Setup](#project-setup)
- [Branching & Commits](#branching--commits)
- [Coding Standards](#coding-standards)
- [Testing](#testing)
- [Pull Request Process](#pull-request-process)
- [Issue Reporting](#issue-reporting)
- [Security](#security)
- [Releases](#releases)
- [Community](#community)
- [License](#license)

## Code of Conduct
---------------

By participating, you agree to uphold our **Code of Conduct** (see CODE_OF_CONDUCT.md). Be kind, be constructive, and help create a welcoming environment for all contributors.

## Ways to Contribute
------------------

- **Bug fixes** – small or large issues in the chat interface, API, or data processing
- **Features** – new AI capabilities, charting, portfolio tracking, or data analysis tools
- **Docs** – README, API documentation, setup guides, and code comments
- **DX** – CI/CD improvements, development tooling, configuration, and dev ergonomics
- **Testing** – add coverage for React components, FastAPI endpoints, and data processing
- **Data & Analysis** – improve S&P 500 data collection, processing, or analysis algorithms

## Before You Start
----------------

1. **Search issues** to avoid duplicates and understand existing work
2. For new features or breaking changes, **open an issue** to align on scope and architecture
3. Check labels like `good first issue` and `help wanted` if you're new to the project
4. Review the project structure to understand the client/server architecture

## Project Setup
-------------

**Requirements**
- Node.js **>= 18** (for React frontend)
- Python **>= 3.8** (for FastAPI backend)
- npm **>= 8** or yarn/pnpm
- Git
- TiDB/MySQL database access (for full functionality)
- OpenAI API key (for AI features)

**1) Fork & clone**

```bash
git clone https://github.com/<your-username>/sp500_agentic_ai.git
cd sp500_agentic_ai
git remote add upstream https://github.com/hanumantjain/sp500_agentic_ai.git 
```

**2) Quick setup (Recommended)**

```bash
chmod +x setup.sh
./setup.sh
```

**3) Manual setup**

**Backend (Server):**
```bash
cd server
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp config.template .env
# Edit .env with your API keys and database credentials
```

**Frontend (Client):**
```bash
cd client
npm install
```

**4) Environment Configuration**

Copy the template and configure your environment:
```bash
cp server/config.template server/.env
```

Edit `server/.env` with your credentials:
```env
# OpenAI Configuration
OPENAI_API_KEY=sk-your-openai-api-key-here

# TiDB/MySQL Database Configuration
TIDB_HOST=your-tidb-host.clusters.tidb-cloud.com
TIDB_PORT=4000
TIDB_USER=your-username
TIDB_PASSWORD=your-password
TIDB_DB_NAME=your-database-name
CA_PATH=/path/to/ca-cert.pem
```

**5) Start development servers**

**Backend:**
```bash
cd server
source .venv/bin/activate
uvicorn main:app --reload --port 8000
```

**Frontend:**
```bash
cd client
npm run dev
```

Visit `http://localhost:5173` to see the application.

**6) Verify setup**

Test the API endpoints:
```bash
curl -X POST "http://localhost:8000/hello"
curl -X POST "http://localhost:8000/ask" -F "question=Hello"
```

## Branching & Commits
-------------------

**Branch names**
```bash
feat/<short-desc>
fix/<short-desc>
chore/<short-desc>
docs/<short-desc>
data/<short-desc>
```

**Conventional Commits** (please use):

```bash
feat: add real-time stock price updates to chat interface
fix: resolve file upload error for PDF documents
docs: update README with TiDB Cloud setup instructions
chore: bump FastAPI version and update dependencies
data: add new S&P 500 historical data for Q4 2024
refactor: extract OpenAI API calls into separate service
test: add unit tests for file upload processing
```

Other types: perf, build, ci, style, revert.

## Coding Standards
----------------

**Frontend (React/TypeScript):**
- **TypeScript** first. No implicit any.
- Use **Tailwind CSS** for styling
- Follow React best practices (hooks, functional components)
- Run linting and formatting:
```bash
cd client
npm run lint
```

**Backend (Python/FastAPI):**
- Follow **PEP 8** style guidelines
- Use **type hints** for all function parameters and return types
- Add **docstrings** for functions and classes
- Use **async/await** for database operations
- Run linting:
```bash
cd server
source .venv/bin/activate
flake8 . --max-line-length=100
black .
```

**General Guidelines:**
- Keep functions small and focused
- Add **JSDoc** or **docstrings** for non-obvious logic
- If you add external libraries, justify them in the PR description
- Prefer composition over inheritance
- Handle errors gracefully with proper error messages

## Testing
-------

**Frontend Testing:**
```bash
cd client
npm test
```

**Backend Testing:**
```bash
cd server
source .venv/bin/activate
python -m pytest
```

**Testing Guidelines:**
- Add tests for new logic (unit tests over integration when possible)
- Test API endpoints with various inputs
- Test file upload functionality with different file types
- Don't ship flaky tests. Mock external dependencies when needed
- If the project currently lacks tests, start by adding tests around what you changed

**Manual Testing Checklist:**
- [ ] Chat interface works with text messages
- [ ] File upload works with supported file types
- [ ] AI responses are generated correctly
- [ ] Database queries execute without errors
- [ ] Error handling works gracefully

## Pull Request Process
--------------------

1. **Sync with upstream main**
```bash
git fetch upstream
git checkout main
git rebase upstream/main
```

2. **Create a feature branch** (see naming above)

3. **Keep PRs focused** (aim < ~500 lines diff if possible)

4. **Checklist before opening PR**
   - [ ] Feature/bug linked to an issue
   - [ ] Frontend linting passes (`npm run lint`)
   - [ ] Backend linting passes (`flake8`, `black`)
   - [ ] Type checking passes (`npm run typecheck`, `mypy`)
   - [ ] Build passes (`npm run build`)
   - [ ] Tests added/updated (if applicable)
   - [ ] Docs updated (README/inline comments)
   - [ ] Environment variables documented if new ones added

5. **PR description template**
   - **What & Why**: Brief description of changes
   - **Screenshots/GIF**: For UI changes
   - **API Changes**: If endpoints are modified
   - **Database Changes**: If schema or queries change
   - **Breaking changes?**: Migration notes if applicable
   - **Follow-ups**: Out of scope items for future PRs

6. **Reviews**
   - Be responsive to feedback; small iterative commits > giant rewrites
   - Squash commits on merge (preserves clean history)
   - Test the PR locally before requesting review

## Issue Reporting
---------------

**Bug report checklist**
- [ ] Repro steps (numbered)
- [ ] Expected vs actual behavior
- [ ] Logs / screenshots / error messages
- [ ] Environment (OS, browser, Node/Python version)
- [ ] Minimal reproduction if possible
- [ ] Backend logs from server terminal

**Feature requests**
- [ ] Problem statement (what's painful now)
- [ ] Proposed solution (API/UI sketch)
- [ ] Alternatives considered
- [ ] Impact (users, performance, developer experience)
- [ ] Integration with existing S&P 500 data and AI features

## Security
--------

If you discover a vulnerability, **do not** open a public issue.

**Security Contact:**
- Email: hanumantjain939@gmail.com, omkarbalekundri77@gmail.com, suprit77@gmail.com
- Include: Description, steps to reproduce, potential impact

We'll acknowledge within 48–72 hours and work on a fix. See SECURITY.md for details.

**Security Considerations:**
- Never commit API keys or database credentials
- Use environment variables for sensitive configuration
- Validate all file uploads
- Sanitize user inputs for database queries
- Follow OWASP guidelines for web security

## Releases
--------

- Use **SemVer**: major.minor.patch
- Add entries to CHANGELOG.md (Keep a Changelog style)
- Tag releases: vX.Y.Z
- Use GitHub Releases notes (auto-generated from Conventional Commits if possible)
- Test releases in staging environment before production

## Community
---------

- Discussions/Q&A can live in **GitHub Discussions**
- Labels: `good first issue`, `help wanted` are beginner-friendly
- We welcome contributions from developers of all experience levels
- Join our community to discuss S&P 500 analysis, AI integration, and financial data processing

## License
-------

By contributing, you agree your contributions are licensed under the repository's **MIT License**.

---

**Thank you for contributing to Bluejay AI!** 🚀

Together, we're building the future of AI-powered financial analysis and S&P 500 insights.

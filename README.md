# Project Setup & Usage

A quick, practical guide to get your environment ready and run the scripts.

## Prerequisites

### Software Requirements

- **SQL Server Management Studio (SSMS)** – already set up
- **Python 3.8+** (with pip)
- **VS Code** with GitHub Copilot + Copilot Chat
- **AWS CLI** (for Aurora/DMS interaction)
- **Java 8+** (for Babelfish Compass)
- **Node.js** (optional, for additional tooling)

### OS Packages (Linux)

**Ubuntu/Debian:**
```bash
sudo apt update && sudo apt install -y python3 python3-venv python3-pip git unzip
```

**RHEL/CentOS/Rocky:**
```bash
sudo dnf install -y python3 python3-pip python3-virtualenv git unzip
```

## VS Code Setup

### Install GitHub Copilot

1. Open VS Code
2. Go to Extensions (`Ctrl+Shift+X`)
3. Install "GitHub Copilot" and "GitHub Copilot Chat"
4. Sign in with your GitHub account (Copilot subscription required)

### Enable Claude Models in Copilot Chat

1. Open Copilot Chat panel
2. Use the model selector
3. Choose "Claude 3.5 Sonnet" for advanced tasks

> **Note:** Claude 4 support is coming soon to VS Code.

## Clone the Repository

**HTTPS:**
```bash
git clone https://github.com/<your-org>/<your-repo>.git
cd <your-repo>
```

**SSH:**
```bash
git clone git@github.com:<your-org>/<your-repo>.git
cd <your-repo>
```

## Python Virtual Environment

### Windows (PowerShell)

```powershell
# From the repo root
python -m venv .venv
.\.venv\Scripts\Activate.ps1

# Upgrade pip (recommended)
python -m pip install --upgrade pip

# Install requirements
pip install -r requirements.txt
```

### Linux/macOS (Bash/Zsh)

```bash
# From the repo root
python3 -m venv .venv
source .venv/bin/activate

# Upgrade pip (recommended)
python -m pip install --upgrade pip

# Install requirements
pip install -r requirements.txt
```

## Environment Variables

1. Copy the example env file:
   ```bash
   cp .env.example .env
   ```
   *(On Windows PowerShell: `Copy-Item .env.example .env`)*

2. Edit `.env` and add your configuration details:

> **Important:** Keep `.env` out of version control. A `.gitignore` entry is usually already provided.

## Java & Babelfish Compass (if applicable)

Ensure Java 8+ is installed:
```bash
java -version
```

Follow your Babelfish Compass instructions or run the provided script in `tools/` if present.

## Node.js (optional)

Some helper tools or UI dashboards may require Node:
```bash
node -v
npm -v

# If needed:
# nvm install --lts
# nvm use --lts
```

## Running the Scripts

From the repo root with the virtual environment activated:

```bash
python scripts/collect_and_analyze.py

# Or anything else in scripts/
python scripts/<your-script>.py --help
```

## Common Troubleshooting

### Virtual env not activating (Windows)
```powershell
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
.\.venv\Scripts\Activate.ps1
```

### pip conflicts / missing wheels
```bash
python -m pip install --upgrade pip setuptools wheel
```

### AWS permissions
Make sure your IAM user/role has access to RDS, DMS, and (if used) Bedrock.

## Project Hygiene

- Keep your `.env` up to date and never commit secrets
- Use feature branches for changes:
  ```bash
  git checkout -b feat/<short-description>
  git commit -m "feat: <message>"
  git push -u origin feat/<short-description>
  ```
- Run scripts inside the activated venv every time

## Quick Recap (TL;DR)

1. Install prerequisites (Python 3.8+, VS Code + Copilot, AWS CLI, Java, Node optional)
2. Clone repo → create and activate venv → `pip install -r requirements.txt`
3. `cp .env.example .env` → fill your config
4. Run scripts with `python scripts/<name>.py` (outputs go to `outputs/`)

---

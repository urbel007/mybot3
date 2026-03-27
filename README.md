# mybot3

## Prerequisites

- **Python 3.10+** (Python 3.11 recommended)
- **pip** and **venv** (included in modern Python installs, `python -m ensurepip --upgrade`)
- **Visual Studio Code (VS Code)** recommended, but not required  
  - Python extension  
  - optional: Pylance, Black, isort  
- (optional) **Git** if you clone from a repo
- (optional) ripgrep (install via powershell: winget install --id BurntSushi.ripgrep.MSVC --exact)

## Install & Run

**Note:**
- PowerShell is supported; if script execution is blocked, activate the venv after adjusting your local execution policy.
- In VS Code, select the interpreter once via `Python: Select Interpreter` and choose `.venv`.

PS C:\Users\urbel> cd 'C:\Projects\mybot3\'
PS C:\Projects\mybot2> .\.venv\Scripts\activate


# Install runtime deps
pip install -r requirements.txt
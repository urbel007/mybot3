# mybot3

## Prerequisites

- **Python 3.10+** (Python 3.11 recommended)
- **pip** and **venv** (included in modern Python installs, `python -m ensurepip --upgrade`)
- **Visual Studio Code (VS Code)** recommended, but not required  
  - Python extension  
  - optional: Pylance, Black, isort  
- (optional) **Git** if you clone from a repo
- (optional) ripgrep (install via powershell: winget install --id BurntSushi.ripgrep.MSVC --exact)

## get from git

## setup virtual environment
PS C:\Users\urbel> cd 'C:\Projects\mybot3\'
PS C:\Projects\mybot3> python -m venv .venv
PS C:\Projects\mybot3> .\.venv\Scripts\activate

## Install & Run


**Note:**
- PowerShell is supported; if script execution is blocked, activate the venv after adjusting your local execution policy.
- In VS Code, select the interpreter once via `Python: Select Interpreter` and choose `.venv`.

PS C:\Users\urbel> cd 'C:\Projects\mybot3\'
PS C:\Projects\mybot3> .\.venv\Scripts\activate


# Install runtime deps
pip install -r requirements.txt

### Trade-Mechanik

1. **11:45 ET:** SPX-Kurs ablesen, ATM-Strike bestimmen
2. **Credit prüfen:** Iron Butterfly (Wing=15) quotieren. Wenn Credit < $1.175 → **kein Trade**
3. **Einstieg:** Short Call + Short Put (ATM), Long Call (ATM+15), Long Put (ATM-15)
4. **Limit-Order:** Sofort Closing-Order bei 80% des Credits platzieren (= 20% Profit)
5. **Stop-Loss:** Wenn Position 50% des Credits verliert → sofort schließen
6. **15:45 ET:** Falls weder TP noch SL → Position zum Marktpreis schließen
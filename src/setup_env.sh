# Creates a virtual environment in the current directory
python3.12 -m venv venv

# Activates the venv
source ./venv/bin/activate

# Installs package dependencies from requirements.txt
python3.12 -m pip install -r requirements.txt

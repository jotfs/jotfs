install:
	sh ./install.sh

gendata:
	python3 create_data_files.py

pip:
	python3 -m pip install -r requirements.txt

run: install gendata pip
	python3 run.py
build:
	# Build the jotfs binary
	mkdir -p bin
	go build -o ./bin/jotfs ../cmd/jotfs

testdata:
	# Build the testdata binary and generate the data used for the tests
	mkdir -p data
	go build -o ./bin/testdata ../cmd/testdata
	chmod u+x ./bin/testdata
	bash gen_testdata.sh

pip:
	# Install the python packages required for running the simulator
	python -m pip install -r requirements.txt

download-jot-binary:
	# Download the latest release of the jot CLI tool from github
	bash download_jot_binary.sh

run: build testdata pip download-jot-binary
	docker run --rm --name minio-jotfs-integration -p 9004:9000 -d minio/minio server /tmp/data
	sleep 3  # give minio a chance to start
	python run.py -n 30 --seed 15644
	docker stop minio-jotfs-integration

run-local: build testdata pip
	# Runs the integration tests using a locally built version of the jot client. Used
	# during development. Requires the JOT_SRC_DIR environment variable to be set to 
	# your jot source directory.
	bash build_local_jot.sh
	docker run --rm --name minio-jotfs-integration -p 9004:9000 -d minio/minio server /tmp/data
	sleep 3  # give minio a chance to start
	python run.py -n 30 --seed 15644
	docker stop minio-jotfs-integration
all: build
build:
        mkdir -p ./packages
        pip3 freeze > requirements.txt
        pip3 install -r requirements.txt --target ./packages
        touch ./packages/empty.txt
        cd packages && zip -r packages.zip  .
        zip -ur ./packages/packages.zip dependencies -x dependencies/__pycache__/\*
        zip -ur ./packages/packages.zip jobs -x jobs/__pycache__/\*
        zip -ur ./packages/packages.zip resources
        zip -ur ./packages/packages.zip configs
        zip -d ./packages/packages.zip empty.txt
        cp ./packages/packages.zip ./packages.zip
        rm -r ./packages
        rm ./requirements.txt
clean:
        rm -r ./packages.zip
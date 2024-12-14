all: build
build:
        mkdir -p ./packages
        touch ./packages/empty.txt
        cd packages && zip -r packages.zip  .
        zip -ur ./packages/packages.zip dependencies -x dependencies/__pycache__/\*
        zip -ur ./packages/packages.zip jobs -x jobs/__pycache__/\*
        zip -ur ./packages/packages.zip resources
        zip -ur ./packages/packages.zip configs
        cp ./packages/packages.zip ./packages.zip
        rm -r ./packages
clean:
        rm -r ./packages.zip
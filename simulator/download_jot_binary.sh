# Dowload the latest release of the jot CLI for this OS and CPU architecture from github
mkdir -p ./bin

arch=$(uname -i)

if [[ "$OSTYPE" == "linux"* ]]; then
    if [[ "$arch" == "x86_64" ]]; then
        build="linux_amd64"
    else
        echo "unsupported architecture $arch for $OS"
        exit 1
    fi
elif [[ "$OSTYPE" == "darwin"* ]]; then
    if [[ "$arch" == "x86_64" ]]; then
        build="darwin_amd64"
    else
        echo "unsupported architecture $arch for $OS"
        exit 1
    fi
else
    echo "unsupported OS $OSTYPE"
    exit 1
fi

url=$(curl -sL https://api.github.com/repos/jotfs/jot/releases/latest \
    | jq -r ".assets | map(select(.name|test(\"$build\"))) | .[0].browser_download_url")
echo "Downloading $url"
curl -sL --output ./bin/jot.gz $url
gzip -df ./bin/jot.gz
chmod u+x ./bin/jot

echo "jot CLI binary saved to ./bin/jot"
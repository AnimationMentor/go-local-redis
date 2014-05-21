DIR="$( command cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
export PATH=${PATH}:/usr/local/go/bin
export GOPATH=${DIR}
go get -d ./...
go test
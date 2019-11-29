moveLeaderPlugin.so: *.go
	GO111MODULE=on GO_ENABLED=1 go build -race -gcflags '' -ldflags '-X "github.com/pingcap/pd/server.PDReleaseVersion=v4.0.0-alpha-80-g4fb447b-dirty" -X "github.com/pingcap/pd/server.PDBuildTS=2019-11-29 06:49:51" -X "github.com/pingcap/pd/server.PDGitHash=4fb447bbfd214c24fe2e320d953e5daafa52cffb" -X "github.com/pingcap/pd/server.PDGitBranch=plug"' -buildmode=plugin -o moveLeaderPlugin.so *.go

.PHONY : clean

clean:
	rm moveLeaderPlugin.so

preSchedulerPlugin.so: *.go
	GO111MODULE=on GO_ENABLED=1 go build -race -gcflags '' -ldflags ' -X "github.com/hustStt/pd/server.PDGitBranch=plug"' -buildmode=plugin -o preSchedulerPlugin.so *.go

.PHONY : clean

clean:
	rm preSchedulerPlugin.so


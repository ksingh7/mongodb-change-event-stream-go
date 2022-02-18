### Introduction
This repository provides a sample implementation of MongoDB Change Events Streams feature in GoLang.

![](https://raw.githubusercontent.com/ksingh7/blogs/main/posts/assets/mongodb-change-streams.png)

### What is MongoDB Change Stream
Change streams allow applications to access real-time data changes without the complexity and risk of tailing the oplog. Applications can use change streams to subscribe to all data changes on a single collection, a database, or an entire deployment, and immediately react to them. Because change streams use the aggregation framework, applications can also filter for specific changes or transform the notifications at will [Read More](https://docs.mongodb.com/manual/changeStreams)

### Usage

```
# export MongoDB URI

export MONGODB_URI="mongodb+srv://admin:xxxxx@cluster0.ii90w.mongodb.net/myFirstDatabase?retryWrites=true&w=majority"

git clone https://github.com/ksingh7/mongodb-change-events-go.git
cd mongodb-change-events-go
go mod tidy
go run main.go
```
# raft

记录raft大致实现原理和工作流程，安全性证明不进行解释

大致的分为3个部分

- Leader election
- Log replication
- Safety

## raft basics

每一台机器有三种状态

- leader
- follower
- candidate

leader是负责处理所有的客户端请求。

raft将时间分为任意长度的间隔，每一段是一个任期，每个阶段从leader selection开始

leader只能有一个

通过rpc进行通信

## leader selection

raft通过心跳机制出发leader selection

开始的时候，每个机器都是follower，当超出一定时间没有接收到leader的心跳信息，就认为网络中leader出现故障，将自己的状态改为candidate，然后发送RequsetVote向其他server寻求投票，当其得到了多数的投票时，就将状态改为leader，然后持续向其他节点发送心跳信息，来不断延长自己的任期。

如果其他server选举定时器超时后进入新的任期，进行新的选举



此处需要注意，当发送RequsetVote，的到的结果，有三种情况

1. 本candidate胜出
2. 其他candidate提前成为leader
3. 没有server的到大多数投票，没有server胜出



每台机器在一个任期只能投一个候选者

第一种情况最好，变为leader即可

第二种情况，这时候不是简单的放弃选举过程，我们要检查两个leader的任期，

任期是一个标志，与时间相关，我们要选择最新的任期。如果另一个leade任期比自己小，server会拒绝这个请求，并且保持自身的candidate状态。

第三种情况再所有参与者同时发出RequsetVote时比较容易发生，叫做选举分裂。所有的候选者需要更新自己的任期，然后开启新一轮selection。

如果我们没有限制情况三非常容易出现，解决方法时选举超时时间是随机的，这样每个机器发出RequsetVote的时间就容易错开



## log replication

执行过程

1. 客户端向leader发送请求
2. leader将命令追加到自己的日志中
3. 并发所有server执行AppendEntries，将日志追加到其他机器
4. 当日志安全复制之后，leader将日志应用到自己状态及，返回结果



上面要注意，发送命令之后，所有的机器都没有直接执行，只是在日志中记录下来，当完成操作之后才进行执行命令过程

这个执行（apply）过程由leader去决定，这样的日志叫做committed。

日志成功复制到多数机器之后，这条日志就变味已提交状态，此条日志之前的也会被提交，（类似AQR中累计确认



在正常的情况下所有server的日志肯定是相同的，但是系统出现问题之后就可能导致

follower的日志有的比leader多，有的比leader少

为了解决这个问题，raft会强制follower保持leader的日志，在AppendEntries的时候会进行一执行检测，如果检测失败，leader会确认follower与自己相同的最后一条日志，然后让follower删除之后的日志。然后同步leader日志

> 论文刚看到这里肯定有疑问，你把多出来的log删除了，那么那些log记录的信息不会丢失吗，假设
>
> 1（leader）同步给234，23正常但是4没接到同步。下了一轮任期中，4成为了leader，那么就会导致123中的log被删除，用户存的内容被删掉。
>
> 答案之后的safry部分，我们对leader select过程进行了约束，4不会成为leader





## safety

为了解决上面的问题，添加了选举约束，保证特定的机器才能被选为leader，这个leader要包含所有前任已经提交的日志

### election restriction

在选举阶段发送ReqestVote时，需要包含leader的日志信息，如果投票者的日志比候选者的日志新，那么就会拒绝给其投票

比较谁的日志“新”是通过比较日志中最后一条日志的任期和索引，先按任期，后按索引进行比较

## Committing entries from previous terms

提交上一个任期的日志

![](https://pic1.zhimg.com/80/v2-a8b60d365f115ea1f192d76c170e06ab_720w.webp?source=1940ef5c)

这一小节比较难以理解

再上图中a时刻s1是leader，在其复制2的时候宕机，在b时刻，b被选为了leader，并且接受了来自客户端的新的请求，但是没来得及复制就宕机了。c时刻s1又恢复了，且被选为了leader，我们尝试去复制2，因为还没有被标记提交，在将2提交后（但并没有覆盖到s5，因为s5宕机了）。就发生了分支情况，两种极端情况。

1. 4没有复制s1宕机

2. 4正常复制

第一种情况下，如果s5恢复了，那么一定是s5被选为leader，因为3的任期比2新，那么按照log replication原则，2会被覆盖，但是2是被提交的日志，这种操作是不被允许的

第二种情况，4正常复制被提交，那么s5就无法被选为leader，就不会出现这种问题，这种情况就是正常的



我们为了保证会按着情况二进行，我们只会去统计判断当前任期的log是否被提交，但是在复制的时候，会将本任何和上一个任期的log都发出去，如果当前log被提前根据log matching property保证之前的内容也会被提交
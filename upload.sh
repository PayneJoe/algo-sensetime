scp -rp target/ctr-2.1.3.jar yuzx@10.0.8.162:/home/yuzx/ypz/
#spawn scp -rp target/ctr-2.1.3.jar yuzx@10.0.8.162:/home/yuzx/ypz/
#set timeout 300 
#expect "yuzx@10.0.8.162's password:"
#set timeout 300 
#send "yuzx2015\r"
#set timeout 300 
#send "exit\r"
#expect eof

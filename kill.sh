    pid=$(ps axu|grep "./bin/$1" |grep -v grep | awk '{print $2}')

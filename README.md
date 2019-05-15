### 将大数据量文本文件压入MYSQL库
```
   boss给了个需求,要将几个八九千万行的数据文件筛选后重新导入Mysql库.
    数据太乱,拿数据库工具搞不定,自己造轮子吧
	最后算下来效率大概在30W qbs左右没啥敏感数据,写的比较渣,丢上来算当做个备份吧.
```

  ### 环境

```` 
JDK 1.8
连接池是阿里家的druid
以及用了yongzhidai大佬写的一个工具类做文件读取( https://github.com/yongzhidai/ToolClass/tree/master/src/main/java/cn/dyz/tools/file )
````


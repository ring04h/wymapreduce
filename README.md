# wymapreduce
Python map reduce, for dnc-emails word count.   

# 说明
分布式架构, 用于可视化分析维基解密泄露的希拉里的邮件.   
   
> python threading用于并行并发处理，会有非常多没必要的上下文切换，性能非常低下.   
> 建议替换成python3 async, 或者直接换用Golang处理.   
   
# word count
* 词频统计，出现次数最高的词   
* 邮件发送信息关联

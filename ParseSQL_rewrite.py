# -*- coding: utf-8 -*-
"""
Created on Wed Nov  7 15:55:57 2018

@author: boyang
"""
import sqlparse,re,pickle
import pandas as pd
from sqlparse.sql import IdentifierList, Identifier
from sqlparse.tokens import Keyword, DML
content=[]
fun_i=[]
fun_t=[]
flag1=0
flag2=0



name_dict=['$table_target','$table_source1','$table_source2','$table_source3',
'$table_source4','$table_source5','$table_source6','$table_source7',
'$table_source8','$table_source9','$table_source10','$table_source11',
'$table_source13','$table_source14','$table_source15','$table_source17',
'$table_source19','$table_source24','$table_source27','$table_source28',
'$table_source30','$table_source31','$table_source32','$table_source33',
'$table_source35'
]

#
create_raw='''CREATE VOLATILE MULTISET TABLE CIS_ACCT_ALLACC_TEMP,no log
      (
       Statistics_Dt        DATE              NOT NULL    --统计日期
       ,Cust_Id             VARCHAR(50)       NOT NULL    --客户号
       ,Currency_Cd         VARCHAR(3)        NOT NULL    --货币代码
       ,Prod_Cd             VARCHAR(200)       NOT NULL    --产品代码
       ,Org_Id              VARCHAR(8)        NOT NULL    --机构编号
       ,Biz_Int_Org_Id      VARCHAR(50)       NOT NULL    --营业机构编号
       ,Portfolio_Id        VARCHAR(50)       NOT NULL    --投资组合编号
       ,Acct_Num            VARCHAR(50)       NOT NULL    --账户号
       ,Acct_Modifier_Num   VARCHAR(10)       NOT NULL    --帐户号修饰符
       ,Bal_Amt             DECIMAL(18,2)     NOT NULL    --余额
       ,Year_Avg_Bal        DECIMAL(18,2)     NOT NULL    --年均余额
       ,Quarter_Avg_Bal     DECIMAL(18,2)     NOT NULL    --季均余额
       ,Month_Avg_Bal       DECIMAL(18,2)     NOT NULL    --月均余额
       ,accd_type           CHAR(2)           NOT NULL    --数据来源
      )
PRIMARY INDEX (Acct_Num ,Acct_Modifier_Num)
ON COMMIT PRESERVE ROWS;
.IF ERRORCODE <> 0 THEN .QUIT 12; 
'''

insert_raw = '''INSERT INTO CIS_ACCT_ALLACC_TEMP
SELECT Statistics_Dt     

FROM   $table_source10      
WHERE  Statistics_Dt = cast( '$TX_DATE' as date format 'YYYYMMDD') 
 group by 1,2,3,4,5,6,7,8,9,14
;'''

# function definations

# split blocks
def split_block(fun_part):
    split_part=[]
    for item in sqlparse.split(''.join(fun_part)): 
        split_part.append(item)    
# remove blank
    while '' in split_part:
        split_part.remove('') 
    return split_part

#file_parse = sqlparse.parse(split_i[3])

def print_tokens(file_parse):
    for token in file_parse[0].tokens:     
        print(type(token), token.ttype, token.value)    
    
def print_identifier(file_parse):
    identifier_list = file_parse[0].tokens[14]    
    for identifier in identifier_list.get_identifiers():
        print(type(identifier), identifier.ttype, identifier.value)

def print_where(file_parse):      
    where = file_parse[0].tokens[18]
    for token in where.tokens:
        print(type(token), token.ttype, token.value)

def print_parent(file_parse):        
    for item in file_parse[0].flatten():   
        if item.ttype is sqlparse.tokens.Keyword.DML and item.value.upper() == 'SELECT':    
           print(item.parent)        
    
def select_insert(data): 
    insert_funs=[]
    for i in data:
       file_parse = sqlparse.parse(i)
       for item in file_parse[0].flatten():
           if item.ttype is sqlparse.tokens.Keyword.DML and item.value.upper() == 'INSERT':    
               insert_funs.append(i)     
    return insert_funs

def select_create(data): 
    insert_funs=[]
    for i in data:
       file_parse = sqlparse.parse(i)
       for item in file_parse[0].flatten():
           if item.ttype is sqlparse.tokens.Keyword.DDL and item.value.upper() == 'CREATE':    
               insert_funs.append(i)     
    return insert_funs

def find_create_line(data):
    for item in data.split('\n'):
        if item.split()[0].upper() == 'CREATE':
           return item 

def find_table_name(data):
    index=data.upper().split().index('TABLE')
    result = data.split()[index+1]
    if ',' in result:
        result=result.split(',')[0]
    return result
              
def subselect_extract(data):
    name = [] 
    mark = []
    name_removed=[]
    if data.tokens[0].is_group and len(data.tokens)>1:
        mark =data.tokens[-1].value
        for token in data:
            if token.is_group:           
               from_seen = False
               for token_inner in token:
                  if isinstance(token_inner,Identifier) and from_seen:
                     name.append(token_inner.value)                    
                  if str(token_inner.ttype).split('.')[-1]=='Placeholder' and from_seen:
                     name.append(token_inner.value)                                  
                  if token_inner.ttype is Keyword and token_inner.value.upper() == 'FROM' :
                       from_seen = True
    for item in name:
        if item in name_all:
            name_removed.append(item)
          
    if len(name_removed)==1: name_removed = name_removed[0] # if only have one name, remove[]     
         
    return [name_removed,mark]

def format_unite(df):
    if type(df) == str:
       return [df] 
    if type(df) == list:
       return df 

def route_for_variable(variable,table):
    result=[]
    tem_table = df_show[df_show['target_table']==table]
    insert_line = tem_table[tem_table['target_variable']==variable] 
    if 'table_source' in insert_line['source_table'].iloc[0][0]:
        
        
        
    return result
    
# extract the main function
with open('cis_cust_imp_info0200.txt', 'r') as f:
    for line in f.readlines():
        if line[0] == '#' or len(line) == 0 or len(line) == 1:
            continue
        content.append(line)
        if line.strip() =='sub BTEQ_I':
            flag1=1
        if line.strip() =='sub BTEQ_T':
            flag2=1
        if flag1==1:
            fun_i.append(line)
        if flag2==1:
            fun_t.append(line)
        if line.strip() =='return $BTEQ_CMD_T;':
            flag2=0   
        if line.strip() =='return $BTEQ_CMD_I;':
            flag1=0   

# split_bolck                
split_i = split_block(fun_i)  
insert_part = select_insert(split_i)
create_part = select_create(split_i)    

# get all table names
tables_list=[]
for item in create_part:
    tables_list.append(find_table_name(find_create_line(item)))
name_all= name_dict+tables_list

# extract route    
route = []      
debug_count=-1 
for part in insert_part:  
    debug_count += 1
    insert_var = []
    select_var = []
    from_table = []
    insert_tar = []
    from_table_mark = []
    where=[]
    flag = 0
    cmd = sqlparse.parse(part)
    for token in cmd[0].tokens:
        
        token_list = cmd[0]
        # set flags for four processes
        if str(token.ttype) == "Token.Keyword.DML" and str(token.value.upper()) == 'INSERT':
           flag = 1 # insert
           continue
        if str(token.ttype) == "Token.Keyword.DML" and str(token.value.upper()) == 'SELECT':   
           flag = 2 # select
           continue
        if str(token.ttype) == "Token.Keyword" and str(token.value.upper()) == 'FROM':
           flag = 3 # from 
           from_index = token_list.token_index(token)
        if str(type(token)).strip("<>'").split('.')[-1] == 'Where':
           flag = 4 
           
        # get contents
        if flag == 1:            
           if str(type(token)).strip("<>'").split('.')[-1] == 'Identifier':
               insert_tar = token.value
           if token.is_group:  # group item
              for item in token.tokens:
                  if str(item.ttype) == 'Token.Name.Placeholder': 
                      insert_tar = item.value
                  if str(type(item)).strip("<>'").split('.')[-1] == 'Identifier':
                      insert_tar = item.value
                  if str(type(item)).strip("<>'").split('.')[-1] == 'Parenthesis': 
                      for j in item:
                          if str(type(j)).strip("<>'").split('.')[-1] == 'IdentifierList':
                             tem=[]
                             for identifier in j.get_identifiers():
                                 tem.append(identifier.value) 
                      insert_var = tem
                                            
        if flag == 2:
           if str(type(token)).strip("<>'").split('.')[-1] == 'IdentifierList':
               tem=[]
               for identifier in token.get_identifiers():
                   tem.append(identifier.value)
               select_var=tem 
           elif flag ==2 and str(type(token)).strip("<>'").split('.')[-1] == 'Identifier':   
               select_var = token.value
                                    
        if flag == 3:             
           if str(type(token)).strip("<>'").split('.')[-1] == 'Identifier':
              if token.is_group and len(token.tokens) > 1:
                 if token.tokens[0].is_group:                   
                     from_table_mark.append(subselect_extract(token)) 
                 else:
                     from_table_mark.append([token[0].value,token[-1].value])
#                     if len(from_table_mark)==1:
#                         from_table_mark=from_table_mark[0]
              else:    
                 from_table_mark.append(token.value)
           elif str(token.ttype).strip("<>'").split('.')[-1] == 'Placeholder':   
              from_table.append(token.value)
              
        if flag == 4:
           where.append(token.value)   
    
    from_table_checked=[]      
    for item in  from_table:
        if item in name_all:
           from_table_checked.append(item) 
    route.append ([insert_var,insert_tar,select_var,from_table_checked,from_table_mark,where])    

def wash_variable(data):
    result=[]
    for i in str(data).split(','):
        if i =='\\n':
            continue    
        i = i.strip(',')         
        i = i.strip('[')       
        i = i.strip(']')
        i = i.strip("'")
        
        while '\\n' in i:
            i=i.replace('\\n','')
        result.append(i.strip())
    return result

def find_route_1(route):
    route2=[]
    v_d=[]  # variable of destination
    table_d=[]  # table of destination
    v_s=[]  # variable of source
    table_s=[]  # variable of source
    
    for i in route:
       table_s=[]
       v_d = wash_variable(i[0])
       table_d = i[1]
       v_s =i[2]
                           
       # pick the table name from name dict
       if len(i[3])==0:
         for item in i[4]:   # only have table name
          if type(item) is str:    
              table_s.append([item,'none'])
          elif len(item)==2:  # have table name and mark
              table_s.append(item)
          
       elif len(i[4])==0 and len(i[3])==1:   
          table_s.append([i[3][0],'none'])
       elif len(i[3]) == len(i[4]):
          tem=[] 
          for num in range(len(i[3])):
              tem.append([i[3][num],i[4][num]])
          table_s.append(tem[0])
       elif len(i[3]) != len(i[4]) and len(i[3])!=0 and len(i[4])!=0:
          for num,item in enumerate(i[4]):
            if type(item) is str:
                table_s.append([i[3][num],item])
            elif len(item)==2:
                table_s.append(item)
       else: table_s = 'error'         
       if v_d == ['']: v_d = v_s  # if v_d has no variable ,set it to v_s
       index = []
       for num,item in enumerate(v_s):  # may cause error
           if item[-2:] == '--':
               index.append(num+1)
       for i in index:
           v_s.pop(i)
       for num,item in enumerate(v_d):
           if item[-2:] == '--':
               index.append(num+1)
       for i in index:
           v_d.pop(i)    
       v_d = [x.split('--')[0].strip().split('\\t')[0].strip("'").split('\t')[0] for x in v_d]
       v_s = [x.split('--')[0].strip().split('\\t')[0].strip("'").split('\t')[0] for x in v_s]              
       
       route2.append([v_d,table_d,v_s,table_s])                   
    return route2  
route2=find_route_1(route)      

#############method 1 for find corresponding mark for variables  
def cor_table(mark,text,num):
    result=[]
    if 'where' in text:
        text = text[text.index('from'):text.index('where')]
    else:
        text = text[text.index('from'):]
    for item in mark:
        index = text.index('t'+item)
        result.append(text[index-1])
    if result[0] == ')':
       result=[]        
       s_list = route2[num][3]
       for j in s_list:
          if 't'+ mark[0] in j:
             for tokens in sqlparse.parse(j)[0].tokens[0].tokens[0].tokens:
                 if str(type(tokens)).strip("<>'").split('.')[-1] == 'Identifier':
                     result.append(str(tokens))
    return result
     
# source data index list
#t_list = []
#table_list = []
#for num,item in enumerate(route2): 
#    tem=[]
#    tem2=[]      
#    for j in item[2]:
#        pattern = re.compile(r'[t|T](\d+)[.]')
#        try:
#            tem.append(re.findall(pattern, j))
#        except:
#            tem.append('this variable has no term')
#        try:  
#            tem2.append(list(set(cor_table(tem[-1],insert_part[num].split(),num))))
#        except:
#            tem2.append('this term has no table')                                  
#    t_list.append(tem)          
#    table_list.append(tem2) 

########method 2
t_list = []
table_list = []
for num,item in enumerate(route2): 
    tem=[]
    tem2=[]  
    mark_list = [x[1] for x in item[3]]    
    for j in item[2]:   
        subtem=[]
        subtem2=[]
        #pattern = re.compile(r'[t|T](\d+)[.]')
        try:
            if len(item[3])==1 :
                tem.append(0)
                tem2.append(item[3][0][0])
                continue
            for mark in mark_list:
                if mark+'.' in j:   # if mark in variable                         
                    subtem.append(mark_list.index(mark))
                    subtem2.append(item[3][mark_list.index(mark)][0])
            tem.append(subtem)
#            if len(subtem2)==1:
#               tem2.append(subtem2[0])
#            elif len(subtem2)>1:
            tem2.append(subtem2) 
        except:
            tem.append('this variable has no term')
            tem2.append('error')
#        try:  
#            tem2.append(list(set(cor_table(tem[-1],insert_part[num].split(),num))))            
#        except:
#            tem2.append('this term has no table')                                  
    t_list.append(tem)          
    table_list.append(tem2) 

# transfer into dataframe
df_data=[]
for index1,insert_cmd in enumerate(route2):
    for index2,variable in enumerate(insert_cmd[0]):
        try:
            df_data.append([variable,insert_cmd[1],insert_cmd[2][index2],insert_cmd[3],table_list[index1][index2],route[index1][5]])
        except:
            print(index1,index2)
df_show=pd.DataFrame(df_data)
df_show.columns=['target_variable','target_table','source_variable','mapping table','source_table','where part']
df_show[4] = df_show[4].apply(lambda x: format_unite(x))

# combine source table
source1 = list(df_show[3])  # intermediate source  
source2 = list(df_show[4])  # intermediate source
source3 = []  # final table
       
test=[]

#for item in source3:    
#    variable_target=[]
#    table_target=[]
#    table_target2=[]
#    if item==[] or  item ==  'this term has no table'  :
#        test.append([])
#        continue
#         
#    if 'select' in item[0]:
#       for j in sqlparse.parse(item[0])[0].tokens[0].tokens[0].tokens:
#             if str(type(j)).strip("<>'").split('.')[-1] == 'Identifier':
#                variable_target.append(j.value) 
#       for z in variable_target:
#           text = item[0].split()
#           table_target.append(text[text.index(z)-1].split('.')[-1].split()[0])                 
#       test.append(table_target)    
#    else:
#        for j in item:
#            wash_va = j.split('.')[-1]
#            wash_va = wash_va.split()[0]
#            table_target2.append(wash_va)
#        test.append(table_target2)
#              
#df_show['source_table']= test

########## generate route in list
all_table =list(set([x for x in df_show[1]]))
all_source = []
for item in all_table:
    tem = df_show[df_show[1]==item][4].tolist()
    #tem = list(set([i for k in tem for i in k]))
    try:
        tem = list(set(tem))
    except:
        tem=tem[0]
    all_source.append(tem)

########## gernerate code
generated_code=[]
for item in create_part:  # created in script
    tem=[]
    for line in item.split('\n')[:-2]:  # skip last two rows
        if line[:2] =='--':
            continue           
        line_split=line.split()   
        if line_split ==[]:
            continue
        if line_split[0].upper()=='CREATE':
           if ',' in line:
                line=line[:line.index(',')]  
                line_split=line.split()     
           tem.append(line_split[0]+' '+line_split[3]+' '+line_split[4]) 
        else:
            tem.append(line)   
    tem.append(';')
    if len(tem) ==3 and '(' not in tem[1]:  # create would fail if no bracket
        tem[1]='()'
    tem='\n'.join(tem)            
    generated_code.append(tem)
    
for item in name_all:   # create source table not in create part
    tem=[]
    tem.append('CREATE TABLE' + item)
    tem.append('()')
    tem.append(';')
    tem='\n'.join(tem)  
    generated_code.append(tem)
    
    
#template='''alter table Table_3
#              add constraint FK_TABLE_3_REFERENCE_CIS_IMP_TEMP02 foreign key ()
#                  references CIS_IMP_TEMP02;'''

# generate code for powerdesign to draw figures

generate_relate = []                  
for num,item in enumerate(all_table):
    for inner in all_source[num]:
        line1 = "alter table "+inner+'\n'
        line2 = "   add constraint FK_" + item +"_REFERENCE_"+item +" foreign key ()\n"
        line3 = "      references " + item + ';\n' 
        generate_relate.append(line1+line2+line3)    
      
# write into txt
f1 = open('generate_code.sql','w')
for item in generated_code:    
    f1.write(item+'\n')
for item in generate_relate:
    f1.write(item+'\n')    
f1.close()

#save df_show for checking
#f=open('check_table.bin','wb')
#pickle.dump(df_show,f)    
#f.close()

with open('check_table.bin', 'rb') as f:
    check_table = pickle.load(f)
    
def get_variable_route(variable,table):
    pass
    











    






    
    
    
    
    
    
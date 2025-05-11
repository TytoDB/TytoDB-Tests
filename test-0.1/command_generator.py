import random, secrets, base64
alphabet = "qwertyuiopasdfghjklzxcvbnmQWERTYUIOPASDFGHJKLZXCVBNM"
types = ['INT','FLOAT','BIGINT','CHAR','BOOL','NANO-STRING','SMALL-STRING','MEDIUM-STRING','BIG-STRING',"LARGE-STRING","NANO-BYTES","SMALL-BYTES","MEDIUM-BYTES","BIG-BYTES","LARGE-BYTES"]
number_operators = ['>=','==','<=','>',"<","!=",'=']
string_operators = ['=','==','!=','&>','&&>','&&&>']
byte_operators = ['=','==','!=']
operators = {
    'INT' : number_operators,
    'BIGINT' : number_operators,
    'FLOAT' : number_operators,
    'BOOL' : ['==','!=','='],
    'TEXT' : string_operators,
    'NANO-STRING' : string_operators,
    'SMALL-STRING' : string_operators,
    'MEDIUM-STRING' : string_operators,
    'BIG-STRING' : string_operators,
    'LARGE-STRING' : string_operators,
    'CHAR' : string_operators,
    'NANO-BYTES' : byte_operators,
    'SMALL-BYTES' : byte_operators,
    'MEDIUM-BYTES' : byte_operators,
    'BIG-BYTES' : byte_operators,
    'LARGE-BYTES' : byte_operators,
}

def gen_str(length = 10) -> str:
    s = ""
    for i in range(length):
        s += alphabet[random.randint(0,len(alphabet)-1)]
    return s 

def gen_b64_bytes(length = 10) -> str:
    return base64.b64encode(random.randbytes(length)).decode('utf-8')

def random_f64():
    sign = 1 if random.random() > 0.5 else -1
    exponent = random.randint(-1022, 1023)
    mantissa = random.random()
    return sign * mantissa * (2.0 ** exponent)

def get_rand_val_from_type(type_name):
    if type_name == "INT":
        return(str(random.randint(-2_147_483_647, 2_147_483_647)))
    elif type_name == "FLOAT":
        return (str(random_f64()))
    elif type_name == "BIGINT":
        return(str(random.randint(-9_223_372_036_854_775_807, 9_223_372_036_854_775_807)))
    elif type_name == "BOOL":
        return(str(random.choice([True, False])).upper()) 
    elif type_name == "CHAR":
        return(f"'{gen_str(1)}'")
    elif type_name == "NANO-STRING":
        return(f"'{gen_str(random.randint(1, 10))}'")
    elif type_name == "SMALL-STRING":
        return(f"'{gen_str(random.randint(1, 100))}'")
    elif type_name == "MEDIUM-STRING":
        return(f"'{gen_str(random.randint(1, 500))}'")
    elif type_name == "BIG-STRING":
        return(f"'{gen_str(random.randint(1, 2000))}'")
    elif type_name == "LARGE-STRING":
        return(f"'{gen_str(random.randint(1, 3000))}'")
    elif type_name == "NANO-BYTES":
        return(f"'{gen_b64_bytes(random.randint(1, 10))}'")
    elif type_name == "SMALL-BYTES":
        return(f"'{gen_b64_bytes(random.randint(1, 100))}'")
    elif type_name == "MEDIUM-BYTES":
        return(f"'{gen_b64_bytes(random.randint(1, 500))}'")
    elif type_name == "BIG-BYTES":
        return(f"'{gen_b64_bytes(random.randint(1, 2000))}'")
    elif type_name == "LARGE-BYTES":
        return(f"'{gen_b64_bytes(random.randint(1, 3000))}'")
    return f"'{gen_str(10)}'" 

def create_conditions(container_columns, container_types) -> str:
    command = ""
    chunks = min(random.randint(1, max(1, len(container_columns)-1)), len(container_columns))
    
    for index in range(chunks):
        if index != 0:
            command += f" {secrets.choice(['AND','OR'])} "
        name = container_columns[index]
        val = container_types[index]
        if val in operators:
            cond = secrets.choice(operators[val])
        else:
            cond = "=" 
        command += f"'{name}' {cond} {get_rand_val_from_type(val)}"
    
    if not command:
        index = 0
        name = container_columns[index]
        val = container_types[index]
        cond = "="
        command = f"'{name}' {cond} {get_rand_val_from_type(val)}"
        
    return command


class CommandGenerator:
    def CreateContainer():
        name = gen_str()
        rang = random.randint(3, 10)
        column_names = [gen_str(random.randint(1, 10)) for _ in range(rang)] 
        column_types = [secrets.choice(types) for _ in column_names]
        assert len(column_names) == len(column_types), \
       f"column/type length mismatch: {len(column_names)} vs {len(column_types)}"

        quoted_columns = [f"'{col}'" for col in column_names]
        quoted_typos = [f"{col}" for col in column_types]
        
        command = f"CREATE CONTAINER '{name}' [{','.join(quoted_columns)}][{','.join(quoted_typos)}]"
        return (command, (name, column_names, column_types))
        
    def CreateRow(container, container_columns, container_types) -> str:
        values = []
        for type_name in container_types:
            values.append(get_rand_val_from_type(type_name))
        command = f"CREATE ROW [{','.join([f"'{col}'" for col in container_columns])}] [{','.join(values)}] ON '{container}'"
        
        return command
    
    def EditRow(container, container_columns, container_types) -> str:
        values = []
        for type_name in container_types:
            values.append(get_rand_val_from_type(type_name))
        command = f"EDIT ROW [{','.join([f"'{col}'" for col in container_columns])}] [{','.join(values)}] ON '{container}' WHERE {create_conditions(container_columns,container_types)}"
        
        return command
        
    def DeleteRow(container, container_columns, container_types) -> str:
        command = f"DELETE ROW [{','.join([f"'{col}'" for col in container_columns])}] ON '{container}' WHERE {create_conditions(container_columns,container_types)}"
        
        return command
        
    def DeleteContainer(container) -> str:
        command = f"DELETE CONTAINER '{container}'"
        
        return command
        
    def SearchRow(container, container_columns, container_types) -> str:
        command = f"SEARCH [{','.join([f"'{col}'" for col in container_columns])}] ON ['{container}'] WHERE {create_conditions(container_columns,container_types)}"
        return command

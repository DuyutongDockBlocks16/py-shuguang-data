import gzip

chunck = "C:\\Users\\wangyb04\\Documents\\51011956476472.bpb.suc.gz"
g_file = gzip.GzipFile(chunck)
bytes = g_file.read()
start = 0
end = 4
a_bi = False
battle_info=''
result=''
# print('---- len_bytes: '+str(len(bytes)))
while True:
    byte_length = int.from_bytes(bytes=bytes[start:end], byteorder='little', signed=True)
    print('---- byte_length: '+ str(byte_length))
    if byte_length == -1:
        a_bi = True
        result=battle_info
        battle_info=''
        start = end
        end = start + 4
    else:
        if a_bi:
            if (byte_length!=0):
                battle_info+=str(bytes[end:(end + byte_length)], 'utf-8')
                battle_info+='\n'
        start = end + byte_length
        end = start + 4

    if start >= len(bytes):
        break

print(result)

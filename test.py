name="anjala"
count=0
for letter in name:
    our_char=[]
    if count(letter)==0:
        print('aaa')
        count+=1
        new_dict={letter:count}
        our_char.append(new_dict)
    count+=1
    if letter in our_char:
        our_char[letter]=count
print(our_char)





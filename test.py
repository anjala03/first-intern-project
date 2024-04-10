# name="anjala"
# my_dict={}
# for letter in name:
#     if letter in my_dict:
#         my_dict[letter]+= 1
#     else:
#         my_dict[letter]=1
# print(my_dict)



my_list=["a","b","c","d","e","f","g","a","v","g"]

dicti={}
for l in my_list:
    print(l)
    if not (l=='a' or l=='g'):
        continue
   
    else:
        if l in dicti:
            dicti[l]+=1
        else:
            dicti[l]=1

    print(dicti)






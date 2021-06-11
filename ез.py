class temp_yield:

    def lloop(self):
        list = [1, 2, 3, 4, 5]
        for i in list:
            return i

a = temp_yield()
df = a.lloop()
print(df)
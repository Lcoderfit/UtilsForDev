import os


if __name__ == '__main__':
    a = os.popen("mysql -u root -plcoder124541").readlines()
    print(a)

import sys
if __name__=="__main__":
    import json
    arg = json.loads(sys.argv[1])
    print(arg)

    #subprocess.checkoutput(["python", "test.py", '{"test":1}'], shell=True)

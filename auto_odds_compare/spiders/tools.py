class MyTools():
    def __init__(self):
        pass

    def list_average(mylist):
        num = len(mylist)
        sum_score = sum(mylist)
        ave_num = round(sum_score / num, 2)
        return ave_num

    def over_threshold_num(mylist, ave_num, threshold_value, direction):
        # direction=1,表示升高方向;   direction=-1,表示降低方向
        if direction == -1:
            over_num = len([i for i in mylist if (i - ave_num) < -threshold_value])
        elif direction == 1:
            over_num = len([i for i in mylist if (i - ave_num) > threshold_value])
        return over_num
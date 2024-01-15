def trans_date(date_str):
    return date_str[0:4] + "-" + date_str[4:6] + "-" + date_str[6:8]
if __name__ == '__main__':

    print(trans_date('20201101'))
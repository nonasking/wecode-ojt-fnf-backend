def get_previous_season(seasons):
    previous_season = [
            str(int(season[0:2])-1) + season[-1]
            for season in seasons
    ]
    return previous_season

if __name__=="__main__":
    print(get_previous_season(["22S","22F","22N"]))


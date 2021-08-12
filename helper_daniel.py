def GetSupport(col):
    """Find out if the candidates ID is mentioned in the tweet

    Parameters
    ----------
    col : type
        The RDD column being passed in for manipulation

    Returns
    -------
    int
        -1: if Le Pen is mentioned
        1: if Macron is mentioned
        0: if neither or both are mentioned
    """
    if le_pen_id in col and macron_id in col:
        return 0
    elif le_pen_id in col:
        return le_pen_support
    elif macron_id in col:
        return macron_support
    else:
        return 0
    pass


def CleanUp(rdd, columns):
    """Only index the useful columns, expand and remove nested columns based on the keys we need

    Parameters
    ----------
    rdd : type
        the RDD we want to clean up
    columns : type
        columns we want to keep

    Returns
    -------
    RDD
        A clean version of the dirty RDD

    """
    return (rdd[columns].withColumn('user_id', rdd['user.id'])
            .withColumn('user_screen_name', rdd['user.screen_name'])
            .withColumn('user_name', rdd['user.name'])
            .withColumn('place_type', rdd['place.place_type'])
            .withColumn('place_coordinates', rdd['place.bounding_box.coordinates'])
            .withColumn('hashtags', rdd['entities.hashtags.text'])
            .withColumn('mentions_id', rdd['entities.user_mentions.id'])
            .drop('user')
            .drop('place')
            .drop('entities')
           )

def coords_estimate(row):
    '''
    params: row: the individual row of the pd dataframe    returns: an array of length 2 with the average x and y coordinates of the bounding box.    '''
    coords = np.array(row[0][0][0])
    return np.mean(coords, axis = 0)   
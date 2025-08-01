def calculate_RSI(data):
    datac = data.copy()
    datac['Return'] = datac['Close'].diff()
    gains = datac[datac['Return'] > 0]['Return']
    loss = datac[datac['Return'] < 0]['Return'].abs()

    mean_gain = gains.mean()
    mean_loss = loss.mean()

    RS = mean_gain/mean_loss

    return round(100 - 100/(1+RS), 2)
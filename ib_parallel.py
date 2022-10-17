import multiprocessing as mp
from multiprocessing import Pool
from functools import partial
import pandas as pd
import numpy as np
from ib_insync import *


def parallelize(data, num_of_processes=4):
    data_split = np.array_split(data, num_of_processes)
    pool = Pool(num_of_processes)
    map_concat = pd.concat(pool.map(partial(run_on_subset, last_live_price), data_split))
    pool.close()
    pool.join()
    return pd.concat([data, pd.DataFrame(map_concat.tolist(),
                                         columns=['Open', 'High', 'Low', 'Last', 'Close', 'prevClose', 'Volume',
                                                  'Time'])],
                                         axis=1)


def parallelize_history(data, num_of_processes=4):
    data_split = np.array_split(data, num_of_processes)
    pool = Pool(num_of_processes)
    mapp = pool.map(partial(run_on_subset, historical_price), data_split)
    pool.close()
    pool.join()
    print(mapp)
    print(data_split)
    return (mapp, data_split)


def run_on_subset(func, data_subset):
    created = mp.Process()
    port = 4002
    client_id_offset = 100
    client_id = created._identity[0] + client_id_offset
    ib = IB()
    ib.connect(host='127.0.0.1', port=port, clientId=client_id)
    print("ib connect port:{} clientId:{}".format(port, client_id))
    # ib.reqMarketDataType(3)
    ret = data_subset.apply(func, args=(ib,), axis=1)
    ib.disconnect()
    print("ib disconnected clientId:{}".format(client_id))
    return ret


def last_live_price(x, ib_args):
    for i in range(10):
        contract = Stock(symbol=x['ticker'], exchange=x['exchange'], currency=x['currency'])
        ib_args.qualifyContracts(contract)
        last_live = ib_args.reqMktData(contract, genericTickList='', snapshot=True, regulatorySnapshot=False,
                                       mktDataOptions=None)
        err = 0

        while ((np.isnan(last_live.close) & np.isnan(last_live.last)) | (
                (last_live.last < 0.1) & (last_live.close < 0.1))) & (err < 10):
            ib_args.sleep(1)
            err = err + 1
        if err < 20:
            print(x['ticker'], " CONFIRM", err, str(last_live))
            if last_live.last > 0.01:
                fixed_last = last_live.last
            else:
                fixed_last = last_live.close
            return last_live.open, last_live.high, last_live.low, fixed_last, last_live.close, None, last_live.volume, last_live.time
    print(x['ticker'], " EXCEPTION", err, str(last_live))


def historical_price(x, ib_args):
    contract = Stock(symbol=x['ticker'], exchange=x['exchange'], currency=x['currency'], primaryExchange='ISLAND')
    historical_data = ib_args.reqHistoricalData(
        contract,
        '',
        barSizeSetting='1 day',
        durationStr='1 Y',
        whatToShow='TRADES',
        useRTH=True,
        keepUpToDate=False
    )
    print(historical_data)
    return util.df(historical_data)


if __name__ == "__main__":
    mp.freeze_support()
    ticker_list = ['TQQQ', 'SQQQ', 'SOXL', 'SPY', 'LABU', 'QQQ', 'XLF', 'UVXY', 'EEM', 'SPXU', 'SPXS', 'HYG', 'EFA', 'IWM', 'SH', 'VEA', 'FNGU', 'ARKK', 'VCSH', 'FXI', 'VWO', 'PSQ', 'XLE', 'EWZ', 'KWEB', 'BND', 'TLT', 'LQD', 'IEFA', 'UPRO', 'TNA', 'SLV', 'IEMG', 'GDX', 'VTEB', 'SOXS', 'TZA', 'SPXL', 'BULZ', 'XLU', 'QID', 'HYLB', 'ICLN', 'IAU', 'SCHF', 'XLI', 'LABD', 'SDS', 'XLV', 'KOLD', 'XBI', 'XLP', 'VIXY', 'EZU', 'KRE', 'XLRE', 'PSLV', 'GOVT', 'TOTL', 'SPTI', 'QLD', 'VGK', 'GDXU', 'IYR', 'VEU', 'BSV', 'XLK', 'GDXJ', 'JNK', 'XLY', 'SPDW', 'MUB', 'VXUS', 'UNG', 'TECL', 'VNQ', 'UVIX', 'JETS', 'NUGT', 'TMF', 'SPLG', 'EWG', 'BOIL', 'BITO', 'XLC', 'EMB', 'EWU', 'AGG', 'PDBC', 'XLB', 'SDOW', 'VCIT', 'BKLN', 'FAZ', 'SJNK', 'SMH', 'SSO', 'EWA', 'BIL', 'SPDN', 'GLD', 'XRT', 'VXX', 'XOP', 'ACWI', 'MCHI', 'SARK', 'WEBL', 'TBT', 'JDST', 'TFI', 'DRIP', 'RWM', 'UUP', 'USHY', 'UDOW', 'HYDW', 'VTI', 'DIA', 'XHB', 'VOO', 'IVV', 'SPLB', 'SCHD', 'IUSB', 'USO', 'YANG', 'QYLD', 'PGX', 'TWM', 'ACWX', 'IJR', 'EWH', 'DUST', 'HIBS', 'SRLN', 'SCHE', 'JEPI', 'SPTL', 'SCHH', 'GLDM', 'VTV', 'EWJ', 'FAS', 'ASHR', 'KBE', 'PFF', 'FEZ', 'EWY', 'URA', 'EPV', 'MSOS', 'TARK', 'VMBS', 'SPAB', 'TIP', 'TECS', 'VT', 'JPST', 'IGSB', 'IEF', 'SHY', 'SCHB', 'SCO', 'XME', 'IWF', 'RSP', 'FDN', 'VGLT', 'IWD', 'SVIX', 'EFV', 'BNDX', 'EWT', 'IXUS', 'SCHR', 'SPTS', 'TBF', 'VTIP', 'ARKW', 'SPIB', 'UCO', 'EWC', 'JNUG', 'SVXY', 'IVW', 'VGSH', 'ARKG', 'VYM', 'VIG', 'IDV', 'SCHP', 'BIV', 'SCHX', 'SPYV', 'SPLV', 'ITOT', 'MBB', 'INDA', 'AGQ']
    df = pd.DataFrame(data={'ticker': ticker_list})
    df['exchange'] = 'SMART'
    df['currency'] = 'USD'
    print(df)

    ib = IB()
    ib.connect(host='127.0.0.1', port=4002, clientId=101)
    # ib.reqMarketDataType(3)

    print(df.apply(last_live_price, args=(ib,), axis=1))

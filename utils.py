from datetime import datetime
utility = {
    'transactions': [{
        '_id': 1,
        'account_id': 470650,
        'transaction_count': 6,
        'bucket_start_date': datetime(1991, 12, 27, 0, 0),
        'bucket_end_date': datetime(2016, 9, 6, 0, 0),
        'transactions': [{
            'date': datetime(2011, 12, 28, 0, 0),
            'amount': 1197,
            'transaction_code': 'buy',
            'symbol': 'nvda',
            'price': '12.7330024299341033611199236474931240081787109375',
            'total': '15241.40390863112172326054861'
        }, {
            'date': datetime(2016, 6, 13, 0, 0),
            'amount': 8797,
            'transaction_code': 'buy',
            'symbol': 'nvda',
            'price': '46.53873172406391489630550495348870754241943359375',
            'total': '409401.2229765902593427995271'
        }, {
            'date': datetime(2016, 8, 31, 0, 0),
            'amount': 6146,
            'transaction_code': 'sell',
            'symbol': 'ebay',
            'price': '32.11600884852845894101847079582512378692626953125',
            'total': '197384.9903830559086514995215'
        }, {
            'date': datetime(2004, 11, 22, 0, 0),
            'amount': 253,
            'transaction_code': 'buy',
            'symbol': 'amzn',
            'price': '37.77441226157566944721111212857067584991455078125',
            'total': '9556.926302178644370144411369'
        }, {
            'date': datetime(2002, 5, 23, 0, 0),
            'amount': 4521,
            'transaction_code': 'buy',
            'symbol': 'nvda',
            'price': '10.763069758141103449133879621513187885284423828125',
            'total': '48659.83837655592869353426977'
        }, {
            'date': datetime(1999, 9, 1, 0, 0),
            'amount': 955,
            'transaction_code': 'buy',
            'symbol': 'csco',
            'price': '27.992136535152877030441231909207999706268310546875',
            'total': '26732.49039107099756407137647'
        }]
    }],
    'customers': [{
        '_id': 1,
        'username': 'lejoshua',
        'name': 'Michael Johnson',
        'address': '15989 Edward Inlet\nLake Maryton, NC 39545',
        'birthdate': datetime(1971, 9, 23, 2, 1, 15),
        'email': 'courtneypaul@gmail.com',
        'accounts': [470650],
        'tier_and_details': {
            'b5f19cb532fa436a9be2cf1d7d1cac8a': {
                'tier': 'Silver',
                'benefits': ['dedicated account representative'],
                'active': True,
                'id': 'b5f19cb532fa436a9be2cf1d7d1cac8a'
            }
        }
    }],
    'accounts': [{
        '_id': '6604f39079da07a9842a7129',
        'account_id': 470650,
        'limit': 10000,
        'products': ['CurrencyService', 'Commodity', 'InvestmentStock']
    }]
}

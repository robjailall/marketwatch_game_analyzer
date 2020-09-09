import csv
import datetime
import glob
import sys
from argparse import ArgumentParser
from collections import defaultdict
from csv import DictWriter
from datetime import datetime
from locale import atof, setlocale, LC_NUMERIC


class Transaction(object):
    def __init__(self, user, symbol, trx_type, trx_date, quantity, price, excluded):
        self.symbol = symbol
        self.trx_type = trx_type
        self.trx_date = trx_date
        self.quantity = quantity
        self.price = price
        self.excluded = excluded
        self.user = user
        self.total_portfolio = 0.0

    def key(item):
        type_map = {
            "buy": "0 buy",
            "sell": "1 sell",
            "short": "2 short",
            "cover": "3 cover"
        }
        return (item.trx_date, item.user, item.symbol, type_map[item.trx_type], item.quantity, item.price)


def num(num_str):
    num_str = num_str.replace("$", "")
    if num_str in ("", "-"):
        return 0.0
    elif "(" in num_str:
        return atof(num_str.replace("(", "").replace(")", ""))
    else:
        return atof(num_str)


def _reduce_stack(stack, quantity):
    amount_left = quantity
    basis = 0
    while amount_left > 0:
        if amount_left > stack[-1][0]:
            basis += stack[-1][0] * stack[-1][1]
            amount_left -= stack[-1][0]
            stack.pop()
        else:
            basis += amount_left * stack[-1][1]
            stack[-1][0] -= amount_left
            amount_left = 0
    return basis


def _calculate_running_portfolio_value(transactions, starting_amount=100_000.00, debug=False):
    user_totals = defaultdict(
        lambda: dict(running_cash_total=starting_amount,
                     running_portfolio_value=0,
                     running_short=0.0,
                     shorted_stack=defaultdict(lambda: []),
                     purchased_stack=defaultdict(lambda: []),
                     quantity=defaultdict(lambda: 0.0),
                     last_price=defaultdict(lambda: 0.0)))

    transactions_sorted = sorted(transactions, key=Transaction.key)

    final_transactions = []

    for t in transactions_sorted:

        if t.excluded:
            continue

        before_cash = user_totals[t.user]["running_cash_total"]
        after_cash = before_cash

        before_portfolio = user_totals[t.user]["running_portfolio_value"]
        after_portfolio = before_portfolio
        after_short_cash = user_totals[t.user]["running_short"]

        if t.trx_type in ("sell"):
            after_cash += t.quantity * t.price

            cost_basis = _reduce_stack(user_totals[t.user]["purchased_stack"][t.symbol], t.quantity)

            # adjust portfolio for still held stocks
            after_portfolio += (t.price - user_totals[t.user]["last_price"][t.symbol]) * \
                               (user_totals[t.user]["quantity"][t.symbol] - t.quantity)

            # adjust portfolio for things being sold
            after_portfolio -= cost_basis
            user_totals[t.user]["quantity"][t.symbol] -= t.quantity
            user_totals[t.user]["last_price"][t.symbol] = t.price
        elif t.trx_type in ("buy"):
            after_cash -= t.quantity * t.price

            user_totals[t.user]["purchased_stack"][t.symbol].append([t.quantity, t.price])

            user_totals[t.user]["quantity"][t.symbol] += t.quantity
            user_totals[t.user]["last_price"][t.symbol] = t.price
            after_portfolio += t.quantity * t.price
        if t.trx_type in ("short"):
            user_totals[t.user]["shorted_stack"][t.symbol].append([t.quantity, t.price])

        elif t.trx_type in ("cover"):
            cost_basis = _reduce_stack(user_totals[t.user]["shorted_stack"][t.symbol], t.quantity)
            after_short_cash += cost_basis - (t.quantity * t.price)

        t.before_cash = before_cash
        t.after_cash = after_cash
        t.before_portfolio = before_portfolio
        t.after_portfolio = after_portfolio
        t.after_short_cash = after_short_cash
        t.total_portfolio = after_cash + after_portfolio + after_short_cash

        user_totals[t.user]["running_cash_total"] = after_cash
        user_totals[t.user]["running_portfolio_value"] = after_portfolio
        user_totals[t.user]["running_short"] = after_short_cash

        if debug:
            print(Transaction.key(t), t.after_cash, t.after_portfolio, t.after_short_cash,
                  t.total_portfolio)

        final_transactions.append(t)

    return final_transactions


def print_user_portfolios_csv(transactions_with_totals, f, starting_total=100_000.00):
    user_names = {}
    idx = 0
    for t in transactions_with_totals:
        if t.user not in user_names:
            user_names[t.user] = idx
            idx += 1

    rows = []
    last_row = {"date": t.trx_date, "user": None, "type": None, "price": None, "quantity": None}
    for u in user_names:
        last_row[u] = starting_total

    for t in transactions_with_totals:
        last_row["date"] = t.trx_date
        last_row[t.user] = t.total_portfolio
        last_row["user"] = t.user
        last_row["symbol"] = t.symbol
        last_row["type"] = t.trx_type
        last_row["price"] = t.price
        last_row["quantity"] = t.quantity
        last_row["total"] = t.price * t.quantity
        rows.append(last_row.copy())

    writer = DictWriter(f=f,
                        fieldnames=["date"]
                                   + list(user_names.keys())
                                   + ["user", "symbol", "type", "price", "quantity", "total"],
                        extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)


def _symbol_included(text, exclude_symbols):
    if exclude_symbols:
        for symbol in exclude_symbols:
            if symbol.upper() in text.upper():
                return False
    return True


def parse_marketwatch_transaction_history(filename, exclude_symbols, debug=False):
    transactions = []

    # Portfolio Transactions - Rob Jay.csv
    user = filename.split("-")[1].strip()[0:-4].lower()

    with open(filename) as f:
        reader = csv.reader(f)
        next(reader)  # header
        for row in reader:
            symbol = row[0]
            excluded = False
            if not _symbol_included(symbol, exclude_symbols):
                excluded = True
                if debug:
                    print("Excluding ", symbol)

            trx_type = row[3].lower()
            trx_date = datetime.strptime("{}m".format(row[2].strip()), "%m/%d/%y %I:%M%p")
            quantity = atof(row[5])
            price = atof(row[6].replace("$", ""))

            transactions.append(Transaction(user, symbol, trx_type, trx_date, quantity, price, excluded))
    return transactions


def _read_symbols_to_set(filename):
    symbols = None
    if filename:
        symbols = set([])
        with open(filename) as f:
            for l in f:
                symbols.add(l.strip())
    return symbols


def main(portfolio_transactions_directory: str, bans_file: str = None, starting_amount=100_000,
         debug=False):
    transactions = []

    exclude_symbols = _read_symbols_to_set(bans_file)

    transaction_history_files = glob.glob("{}/Portfolio Transactions*.csv".format(portfolio_transactions_directory))

    for fn in transaction_history_files:
        transactions.extend(
            parse_marketwatch_transaction_history(filename=fn, debug=debug,
                                                  exclude_symbols=exclude_symbols))

    transactions_with_totals = _calculate_running_portfolio_value(transactions, starting_amount=starting_amount,
                                                                  debug=debug)

    print_user_portfolios_csv(transactions_with_totals=transactions_with_totals, f=sys.stdout,
                              starting_total=starting_amount)


if __name__ == "__main__":
    setlocale(LC_NUMERIC, "en_US.UTF-8")
    parser = ArgumentParser()
    parser.add_argument("input_dir", type=str, default=None,
                        help="Directory with CSV exports of Portfolio Transactions")
    parser.add_argument("--debug", action="store_true", default=False)
    parser.add_argument("--bans", type=str, default=None,
                        help="File with a list of banned stocks")
    parser.add_argument("--starting-amount", type=float, default=100_000)

    args = parser.parse_args()
    main(portfolio_transactions_directory=args.input_dir,
         bans_file=args.bans,
         starting_amount=args.starting_amount,
         debug=args.debug)

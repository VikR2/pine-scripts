#!/usr/bin/env python3
"""
80-Minute Cycle Win Rate Report Generator - Auto-Watch Mode

Watches a directory for new TradingView backtest exports and automatically
generates win rate analysis reports.

Usage:
    python cycle_report_watcher.py [watch_directory] [output_directory]

Examples:
    python cycle_report_watcher.py                          # Watch current directory
    python cycle_report_watcher.py ./backtests              # Watch specific folder
    python cycle_report_watcher.py ./backtests ./reports    # Separate output folder

Press Ctrl+C to stop watching.
"""

import os
import sys
import time
from datetime import datetime
from pathlib import Path

import pandas as pd

# Try to import watchdog, fall back to polling if not available
try:
    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler
    USE_WATCHDOG = True
except ImportError:
    USE_WATCHDOG = False
    print("Note: 'watchdog' not installed. Using polling mode (checks every 2 seconds).")
    print("For better performance, install watchdog: pip install watchdog\n")

# Configuration
SESSION_START_HOUR = 7
SESSION_END_HOUR = 16
CYCLE_DURATION = 80
POINT_VALUE = 100
SUPPORTED_EXTENSIONS = {'.xlsx', '.xls', '.csv'}

CYCLE_TIMES = {
    0: '07:00 - 08:20',
    1: '08:20 - 09:40',
    2: '09:40 - 11:00',
    3: '11:00 - 12:20',
    4: '12:20 - 13:40',
    5: '13:40 - 15:00',
    6: '15:00 - 16:20'
}


def load_data(filepath):
    """Load trade data from Excel or CSV file."""
    ext = os.path.splitext(filepath)[1].lower()
    
    if ext in ['.xlsx', '.xls']:
        all_sheets = pd.read_excel(filepath, sheet_name=None)
        
        trades_sheet = None
        for name in all_sheets.keys():
            if 'trade' in name.lower() and 'list' in name.lower():
                trades_sheet = name
                break
            elif 'trade' in name.lower():
                trades_sheet = name
                
        if trades_sheet:
            df = pd.read_excel(filepath, sheet_name=trades_sheet)
        else:
            df = max(all_sheets.values(), key=lambda x: len(x.columns))
            
        props = {}
        for name in all_sheets.keys():
            if 'propert' in name.lower():
                props_df = pd.read_excel(filepath, sheet_name=name)
                if 'name' in props_df.columns and 'value' in props_df.columns:
                    props = dict(zip(props_df['name'], props_df['value']))
                break
                
        return df, props
        
    elif ext == '.csv':
        df = pd.read_csv(filepath)
        return df, {}
    else:
        raise ValueError(f"Unsupported file format: {ext}")


def process_trades(df):
    """Process trade data and calculate cycle statistics."""
    datetime_col = None
    for col in df.columns:
        if 'date' in col.lower() or 'time' in col.lower():
            datetime_col = col
            break
    
    if datetime_col is None:
        raise ValueError("Could not find Date/Time column in data")
    
    type_col = None
    for col in df.columns:
        if col.lower() == 'type':
            type_col = col
            break
    
    pnl_col = None
    for col in df.columns:
        if 'p&l' in col.lower() or 'pnl' in col.lower() or 'profit' in col.lower():
            if 'usd' in col.lower() or 'net' in col.lower():
                pnl_col = col
                break
    if pnl_col is None:
        for col in df.columns:
            if 'p&l' in col.lower() or 'pnl' in col.lower():
                pnl_col = col
                break
    
    if pnl_col is None:
        raise ValueError("Could not find P&L column in data")
    
    df[datetime_col] = pd.to_datetime(df[datetime_col])
    
    if type_col:
        entries = df[df[type_col].str.contains('Entry', case=False, na=False)].copy()
    else:
        entries = df.copy()
    
    if len(entries) == 0:
        raise ValueError("No trade entries found in data")
    
    entries['hour'] = entries[datetime_col].dt.hour
    entries['minute'] = entries[datetime_col].dt.minute
    entries['date'] = entries[datetime_col].dt.date
    
    entries['total_mins'] = (entries['hour'] - SESSION_START_HOUR) * 60 + entries['minute']
    entries['cycle_num'] = entries['total_mins'] // CYCLE_DURATION
    
    entries = entries[(entries['cycle_num'] >= 0) & (entries['cycle_num'] <= 6)]
    
    entries['is_win'] = entries[pnl_col] > 0
    entries['pnl'] = entries[pnl_col]
    
    if type_col:
        entries['trade_type'] = entries[type_col].apply(
            lambda x: 'Long' if 'long' in str(x).lower() else 'Short'
        )
    else:
        entries['trade_type'] = 'Unknown'
    
    return entries


def calculate_cycle_stats(entries):
    """Calculate statistics for each cycle."""
    stats = []
    
    for cycle in range(7):
        cycle_data = entries[entries['cycle_num'] == cycle]
        
        if len(cycle_data) == 0:
            continue
            
        long_data = cycle_data[cycle_data['trade_type'] == 'Long']
        short_data = cycle_data[cycle_data['trade_type'] == 'Short']
        
        row = {
            'cycle': cycle,
            'time_range': CYCLE_TIMES[cycle],
            'total_trades': len(cycle_data),
            'wins': int(cycle_data['is_win'].sum()),
            'losses': len(cycle_data) - int(cycle_data['is_win'].sum()),
            'win_rate': round((cycle_data['is_win'].sum() / len(cycle_data)) * 100, 1),
            'total_pnl': cycle_data['pnl'].sum(),
            'avg_pnl': round(cycle_data['pnl'].mean(), 0),
            'long_trades': len(long_data),
            'long_wr': round((long_data['is_win'].sum() / len(long_data)) * 100, 1) if len(long_data) > 0 else 0,
            'long_pnl': long_data['pnl'].sum() if len(long_data) > 0 else 0,
            'short_trades': len(short_data),
            'short_wr': round((short_data['is_win'].sum() / len(short_data)) * 100, 1) if len(short_data) > 0 else 0,
            'short_pnl': short_data['pnl'].sum() if len(short_data) > 0 else 0
        }
        stats.append(row)
    
    return pd.DataFrame(stats)


def calculate_overall_stats(entries):
    """Calculate overall trading statistics."""
    long_trades = entries[entries['trade_type'] == 'Long']
    short_trades = entries[entries['trade_type'] == 'Short']
    
    return {
        'total_trades': len(entries),
        'total_wins': int(entries['is_win'].sum()),
        'total_losses': len(entries) - int(entries['is_win'].sum()),
        'overall_wr': round((entries['is_win'].sum() / len(entries)) * 100, 2),
        'total_pnl': entries['pnl'].sum(),
        'long_trades': len(long_trades),
        'long_wins': int(long_trades['is_win'].sum()),
        'long_losses': len(long_trades) - int(long_trades['is_win'].sum()),
        'long_wr': round((long_trades['is_win'].sum() / len(long_trades)) * 100, 2) if len(long_trades) > 0 else 0,
        'long_pnl': long_trades['pnl'].sum() if len(long_trades) > 0 else 0,
        'short_trades': len(short_trades),
        'short_wins': int(short_trades['is_win'].sum()),
        'short_losses': len(short_trades) - int(short_trades['is_win'].sum()),
        'short_wr': round((short_trades['is_win'].sum() / len(short_trades)) * 100, 2) if len(short_trades) > 0 else 0,
        'short_pnl': short_trades['pnl'].sum() if len(short_trades) > 0 else 0,
        'start_date': entries['date'].min(),
        'end_date': entries['date'].max()
    }


def format_pnl(value):
    """Format P&L value as string."""
    if value >= 0:
        return f"${value:,.0f}"
    else:
        return f"-${abs(value):,.0f}"


def format_pnl_signed(value):
    """Format P&L value with +/- sign."""
    if value >= 0:
        return f"+${value:,.0f}"
    else:
        return f"-${abs(value):,.0f}"


def generate_report(cycle_stats, overall_stats, props=None):
    """Generate the markdown report."""
    symbol = props.get('Symbol', 'Unknown') if props else 'Unknown'
    timeframe = props.get('Timeframe', 'Unknown') if props else 'Unknown'
    stop_loss = props.get('Stop Loss (points)', 'N/A') if props else 'N/A'
    take_profit = props.get('Take Profit (points)', 'N/A') if props else 'N/A'
    initial_capital = props.get('Initial capital', 'N/A') if props else 'N/A'
    
    report = f"""# 80-Minute Cycle Strategy - Win Rate Analysis Report

## Strategy Overview

| Parameter | Value |
|-----------|-------|
| **Symbol** | {symbol} |
| **Timeframe** | {timeframe} |
| **Trading Period** | {overall_stats['start_date']} to {overall_stats['end_date']} |
| **Session Hours** | 7:00 AM - 4:00 PM EST |
| **Stop Loss** | {stop_loss} points |
| **Take Profit** | {take_profit} points |
| **Initial Capital** | {initial_capital if isinstance(initial_capital, str) else f"${initial_capital:,.0f}"} |

---

## Overall Performance Summary

| Metric | All Trades | Long | Short |
|--------|------------|------|-------|
| **Total Trades** | {overall_stats['total_trades']} | {overall_stats['long_trades']} | {overall_stats['short_trades']} |
| **Winning Trades** | {overall_stats['total_wins']} | {overall_stats['long_wins']} | {overall_stats['short_wins']} |
| **Losing Trades** | {overall_stats['total_losses']} | {overall_stats['long_losses']} | {overall_stats['short_losses']} |
| **Win Rate** | {overall_stats['overall_wr']:.2f}% | {overall_stats['long_wr']:.2f}% | {overall_stats['short_wr']:.2f}% |
| **Net P&L** | {format_pnl(overall_stats['total_pnl'])} | {format_pnl(overall_stats['long_pnl'])} | {format_pnl(overall_stats['short_pnl'])} |

---

## Cycle-by-Cycle Win Rate Analysis

The trading session (7:00 AM - 4:00 PM EST) is divided into **seven 80-minute cycles**. Each cycle consists of:
- **Phase A (Accumulation)**: First 40 minutes - Range established
- **Phase B (Execution)**: Last 40 minutes - Trade signals generated

### Summary Table

| Cycle | Time (EST) | Trades | Wins | Losses | Win Rate | Total P&L | Avg P&L |
|:-----:|:----------:|:------:|:----:|:------:|:--------:|----------:|--------:|
"""

    for _, row in cycle_stats.iterrows():
        report += f"| {int(row['cycle'])} | {row['time_range']} | {int(row['total_trades'])} | {int(row['wins'])} | {int(row['losses'])} | **{row['win_rate']:.1f}%** | {format_pnl(row['total_pnl'])} | {format_pnl(row['avg_pnl'])} |\n"

    report += """
---

## Cycle Rankings

### By Win Rate (Highest to Lowest)

"""

    ranked = cycle_stats.sort_values('win_rate', ascending=False)
    for rank, (_, row) in enumerate(ranked.iterrows(), 1):
        icons = {1: "[1st]", 2: "[2nd]", 3: "[3rd]"}
        icon = icons.get(rank, f"[{rank}]")
        report += f"**{icon} Cycle {int(row['cycle'])}** ({row['time_range']}) - **{row['win_rate']:.1f}%** win rate | {int(row['total_trades'])} trades | {format_pnl_signed(row['total_pnl'])}\n\n"

    report += """### By Profitability (Best to Worst)

"""

    ranked_pnl = cycle_stats.sort_values('total_pnl', ascending=False)
    for rank, (_, row) in enumerate(ranked_pnl.iterrows(), 1):
        icons = {1: "[1st]", 2: "[2nd]", 3: "[3rd]"}
        icon = icons.get(rank, f"[{rank}]")
        report += f"**{icon} Cycle {int(row['cycle'])}** ({row['time_range']}) - **{format_pnl_signed(row['total_pnl'])}** | {row['win_rate']:.1f}% WR\n\n"

    report += """---

## Long vs Short Performance by Cycle

| Cycle | Time (EST) | Long Trades | Long WR | Long P&L | Short Trades | Short WR | Short P&L |
|:-----:|:----------:|:-----------:|:-------:|---------:|:------------:|:--------:|---------:|
"""

    for _, row in cycle_stats.iterrows():
        report += f"| {int(row['cycle'])} | {row['time_range']} | {int(row['long_trades'])} | {row['long_wr']:.1f}% | {format_pnl(row['long_pnl'])} | {int(row['short_trades'])} | {row['short_wr']:.1f}% | {format_pnl(row['short_pnl'])} |\n"

    best_wr = ranked.iloc[0]
    worst_wr = ranked.iloc[-1]
    best_pnl = ranked_pnl.iloc[0]
    worst_pnl = ranked_pnl.iloc[-1]
    
    high_wr_cycles = cycle_stats[cycle_stats['win_rate'] >= 55].sort_values('win_rate', ascending=False)
    low_wr_cycles = cycle_stats[cycle_stats['win_rate'] < 50].sort_values('win_rate')

    report += """
---

## Key Insights & Recommendations

### HIGH-WIN-RATE CYCLES (Recommended)

| Cycle | Time | Win Rate | Insight |
|:-----:|:----:|:--------:|---------|
"""

    for _, row in high_wr_cycles.head(3).iterrows():
        long_note = f"Longs: {row['long_wr']:.0f}%" if row['long_trades'] > 0 else "No longs"
        short_note = f"Shorts: {row['short_wr']:.0f}%" if row['short_trades'] > 0 else "No shorts"
        report += f"| **{int(row['cycle'])}** | {row['time_range']} | **{row['win_rate']:.1f}%** | {long_note}, {short_note}. P&L: {format_pnl_signed(row['total_pnl'])} |\n"

    report += """
### LOW-WIN-RATE CYCLES (Avoid or Modify)

| Cycle | Time | Win Rate | Issue |
|:-----:|:----:|:--------:|-------|
"""

    for _, row in low_wr_cycles.head(3).iterrows():
        report += f"| **{int(row['cycle'])}** | {row['time_range']} | **{row['win_rate']:.1f}%** | P&L: {format_pnl(row['total_pnl'])}. Consider skipping this cycle. |\n"

    report += f"""
### Strategic Recommendations

1. **Best Cycle**: Cycle {int(best_wr['cycle'])} ({best_wr['time_range']}) has the highest win rate at {best_wr['win_rate']:.1f}%.

2. **Worst Cycle**: Cycle {int(worst_wr['cycle'])} ({worst_wr['time_range']}) has the lowest win rate at {worst_wr['win_rate']:.1f}%. Consider avoiding.

3. **Most Profitable**: Cycle {int(best_pnl['cycle'])} ({best_pnl['time_range']}) generated {format_pnl_signed(best_pnl['total_pnl'])}.

4. **Biggest Loser**: Cycle {int(worst_pnl['cycle'])} ({worst_pnl['time_range']}) lost {format_pnl(worst_pnl['total_pnl'])}.

5. **Direction Bias**: {"Longs outperform shorts" if overall_stats['long_wr'] > overall_stats['short_wr'] else "Shorts outperform longs"} ({overall_stats['long_wr']:.1f}% vs {overall_stats['short_wr']:.1f}%).

---

## Recommended Trading Schedule

| Priority | Cycle | Time (EST) | Win Rate | Action |
|:--------:|:-----:|:----------:|:--------:|--------|
"""

    all_cycles = cycle_stats.sort_values('win_rate', ascending=False)
    priority = 1
    for _, row in all_cycles.iterrows():
        if row['win_rate'] >= 55:
            status = "[GO]"
            action = "Trade both directions"
            if row['long_wr'] > row['short_wr'] + 10:
                action = "Favor longs"
            elif row['short_wr'] > row['long_wr'] + 10:
                action = "Favor shorts"
        elif row['win_rate'] >= 50:
            status = "[CAUTION]"
            action = "Trade with caution"
        else:
            status = "[SKIP]"
            action = "Do not trade"
        
        report += f"| {status} {priority} | {int(row['cycle'])} | {row['time_range']} | {row['win_rate']:.1f}% | {action} |\n"
        priority += 1

    report += f"""
---

*Report generated: {datetime.now().strftime('%B %d, %Y at %H:%M')}*  
*Data source: TradingView Backtest Export*
"""

    return report


def process_file(filepath, output_dir):
    """Process a single file and generate report."""
    filename = os.path.basename(filepath)
    base_name = os.path.splitext(filename)[0]
    output_file = os.path.join(output_dir, f"{base_name}_cycle_report.md")
    
    print(f"\n{'='*60}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] New file detected: {filename}")
    print(f"{'='*60}")
    
    try:
        df, props = load_data(filepath)
        print(f"  Loaded {len(df)} rows")
        
        entries = process_trades(df)
        print(f"  Processed {len(entries)} trade entries")
        
        cycle_stats = calculate_cycle_stats(entries)
        print(f"  Analyzed {len(cycle_stats)} cycles")
        
        overall_stats = calculate_overall_stats(entries)
        
        report = generate_report(cycle_stats, overall_stats, props)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(f"\n  [SUCCESS] Report saved to: {output_file}")
        print(f"\n  Quick Summary:")
        print(f"    Total Trades: {overall_stats['total_trades']}")
        print(f"    Win Rate: {overall_stats['overall_wr']:.1f}%")
        print(f"    Net P&L: {format_pnl(overall_stats['total_pnl'])}")
        print(f"    Best Cycle: {cycle_stats.loc[cycle_stats['win_rate'].idxmax(), 'cycle']:.0f} ({cycle_stats['win_rate'].max():.1f}% WR)")
        print(f"    Worst Cycle: {cycle_stats.loc[cycle_stats['win_rate'].idxmin(), 'cycle']:.0f} ({cycle_stats['win_rate'].min():.1f}% WR)")
        
        return True
        
    except Exception as e:
        print(f"\n  [ERROR] Failed to process: {e}")
        return False


# Watchdog event handler
if USE_WATCHDOG:
    class NewFileHandler(FileSystemEventHandler):
        def __init__(self, output_dir, processed_files):
            self.output_dir = output_dir
            self.processed_files = processed_files
            
        def on_created(self, event):
            if event.is_directory:
                return
            
            filepath = event.src_path
            ext = os.path.splitext(filepath)[1].lower()
            
            if ext not in SUPPORTED_EXTENSIONS:
                return
            
            # Skip if already processed
            if filepath in self.processed_files:
                return
            
            # Wait a moment for file to finish writing
            time.sleep(1)
            
            # Check file is complete (size stable)
            try:
                size1 = os.path.getsize(filepath)
                time.sleep(0.5)
                size2 = os.path.getsize(filepath)
                if size1 != size2:
                    time.sleep(2)  # Wait more if still writing
            except:
                return
            
            self.processed_files.add(filepath)
            process_file(filepath, self.output_dir)
        
        def on_modified(self, event):
            # Also handle modified events for some systems
            if event.is_directory:
                return
            
            filepath = event.src_path
            ext = os.path.splitext(filepath)[1].lower()
            
            if ext not in SUPPORTED_EXTENSIONS:
                return
            
            if filepath in self.processed_files:
                return
            
            time.sleep(1)
            self.processed_files.add(filepath)
            process_file(filepath, self.output_dir)


def watch_with_polling(watch_dir, output_dir):
    """Fallback polling-based file watcher."""
    processed_files = set()
    
    # Get existing files
    for f in os.listdir(watch_dir):
        filepath = os.path.join(watch_dir, f)
        if os.path.isfile(filepath):
            ext = os.path.splitext(f)[1].lower()
            if ext in SUPPORTED_EXTENSIONS:
                processed_files.add(filepath)
    
    print(f"Found {len(processed_files)} existing files (will be skipped)")
    
    while True:
        try:
            for f in os.listdir(watch_dir):
                filepath = os.path.join(watch_dir, f)
                
                if not os.path.isfile(filepath):
                    continue
                
                ext = os.path.splitext(f)[1].lower()
                if ext not in SUPPORTED_EXTENSIONS:
                    continue
                
                if filepath in processed_files:
                    continue
                
                # Wait for file to finish writing
                time.sleep(1)
                try:
                    size1 = os.path.getsize(filepath)
                    time.sleep(0.5)
                    size2 = os.path.getsize(filepath)
                    if size1 != size2:
                        continue  # Still writing
                except:
                    continue
                
                processed_files.add(filepath)
                process_file(filepath, output_dir)
            
            time.sleep(2)  # Check every 2 seconds
            
        except KeyboardInterrupt:
            raise
        except Exception as e:
            print(f"Error scanning directory: {e}")
            time.sleep(5)


def main():
    """Main entry point."""
    # Parse arguments
    watch_dir = sys.argv[1] if len(sys.argv) > 1 else '.'
    output_dir = sys.argv[2] if len(sys.argv) > 2 else watch_dir
    
    # Resolve paths
    watch_dir = os.path.abspath(watch_dir)
    output_dir = os.path.abspath(output_dir)
    
    # Validate directories
    if not os.path.isdir(watch_dir):
        print(f"Error: Watch directory does not exist: {watch_dir}")
        sys.exit(1)
    
    if not os.path.isdir(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")
    
    print("="*60)
    print("  80-Minute Cycle Report Generator - Auto-Watch Mode")
    print("="*60)
    print(f"\n  Watching: {watch_dir}")
    print(f"  Output:   {output_dir}")
    print(f"  Formats:  {', '.join(SUPPORTED_EXTENSIONS)}")
    print(f"\n  Drop a backtest export file to generate a report!")
    print(f"  Press Ctrl+C to stop.\n")
    print("="*60)
    
    if USE_WATCHDOG:
        # Use watchdog for efficient file monitoring
        processed_files = set()
        
        # Mark existing files as processed
        for f in os.listdir(watch_dir):
            filepath = os.path.join(watch_dir, f)
            if os.path.isfile(filepath):
                ext = os.path.splitext(f)[1].lower()
                if ext in SUPPORTED_EXTENSIONS:
                    processed_files.add(filepath)
        
        print(f"Found {len(processed_files)} existing files (will be skipped)")
        
        event_handler = NewFileHandler(output_dir, processed_files)
        observer = Observer()
        observer.schedule(event_handler, watch_dir, recursive=False)
        observer.start()
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n\nStopping watcher...")
            observer.stop()
        observer.join()
    else:
        # Fallback to polling
        try:
            watch_with_polling(watch_dir, output_dir)
        except KeyboardInterrupt:
            print("\n\nStopping watcher...")
    
    print("Goodbye!")


if __name__ == "__main__":
    main()

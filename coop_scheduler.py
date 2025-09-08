import argparse
import pandas as pd
from datetime import datetime, timedelta
import re
import random
import math
from collections import defaultdict
from copy import deepcopy

ELIGIBLE_KEYS = ["off", "career prep", "business management", "bus mgt", "management"]
ALT_KEYWORDS = ["alt", "alternate", "alternative"]

def normalize(s: str) -> str:
    return re.sub(r"\s+", " ", str(s).strip().lower())

def is_eligible(subject: str) -> bool:
    s = normalize(subject)
    return any(k in s for k in ELIGIBLE_KEYS)

def is_alternate(name: str) -> bool:
    """Check if a student is an alternate based on name containing 'alt'"""
    return any(alt_word in normalize(name) for alt_word in ALT_KEYWORDS)

def parse_wide_schedule_str(schedule_str: str) -> dict:
    """
    Turns '1 - career prep, 2 - algebra, 3 - off, ...' into {1:'career prep', 2:'algebra', 3:'off', ...}
    """
    periods = {}
    if not isinstance(schedule_str, str) or not schedule_str.strip():
        return periods
    for entry in schedule_str.split(","):
        entry = entry.strip()
        if not entry:
            continue
        # allow both "1 - X" and "1: X"
        m = re.match(r"^\s*(\d+)\s*[-:]\s*(.+?)\s*$", entry)
        if not m:
            continue
        p = int(m.group(1))
        subj = m.group(2).strip()
        periods[p] = subj
    return periods

def scan_available_periods(students_dict: dict) -> tuple:
    """
    Scan all students to find which periods actually have eligible students.
    Returns (set of A-day periods with students, set of B-day periods with students)
    """
    a_day_periods = set()  # Periods 1-4
    b_day_periods = set()  # Periods 5-8
    
    for student_name, student_data in students_dict.items():
        for period in student_data['eligible_periods']:
            if 1 <= period <= 4:
                a_day_periods.add(period)
            elif 5 <= period <= 8:
                b_day_periods.add(period)
    
    return a_day_periods, b_day_periods

def load_student_periods(df: pd.DataFrame) -> tuple:
    """
    Supports TWO input shapes:
      A) 'wide' with columns: Name, Schedule   (Schedule is '1 - x, 2 - y, ...')
      B) 'long' with columns: Name, Period, Class
    Returns a tuple of (regular_students, alternate_students) dicts:
      { name: {
          'eligible_periods': [ints],
          'off_periods': [ints],
          'career_prep_periods': [ints],
          'last_worked': None,
          'total_shifts': 0,
          'period_counts': defaultdict(int),
          'consecutive_days': 0,
          'max_consecutive': 0
        }, ... }
    """
    df_cols = [c.lower() for c in df.columns]

    # Try to detect shape B (long)
    long_ok = all(col in df_cols for col in ["name", "period", "class"])

    # Try to detect shape A (wide)
    wide_ok = all(col in df_cols for col in ["name", "schedule"])

    if not (long_ok or wide_ok):
        raise ValueError("Input must have either columns [Name, Schedule] or [Name, Period, Class].")

    regular_students = {}
    alternate_students = {}

    def create_student_data():
        return {
            "eligible_periods": [],
            "off_periods": [],
            "career_prep_periods": [],
            "last_worked": None,
            "total_shifts": 0,
            "period_counts": defaultdict(int),
            "consecutive_days": 0,
            "max_consecutive": 0,
            "days_worked": set(),
            "recent_periods": [],
            "fairness_score": 0.0
        }

    if wide_ok:
        # normalize columns
        df2 = df.rename(columns={c: c.lower() for c in df.columns})
        for _, row in df2.iterrows():
            name = str(row["name"]).strip()
            sched_map = parse_wide_schedule_str(row["schedule"])
            eligible = [p for p, subj in sched_map.items() if is_eligible(subj)]
            off_periods = [p for p, subj in sched_map.items() if "off" in normalize(subj)]
            career_prep_periods = [p for p, subj in sched_map.items() if "career prep" in normalize(subj)]
            
            student_data = create_student_data()
            student_data.update({
                "eligible_periods": sorted(set(eligible)),
                "off_periods": sorted(set(off_periods)),
                "career_prep_periods": sorted(set(career_prep_periods))
            })
            
            if is_alternate(name):
                alternate_students[name] = student_data
            else:
                regular_students[name] = student_data

    if long_ok:
        df3 = df.rename(columns={c: c.lower() for c in df.columns})
        # Ensure Period is int
        df3["period"] = df3["period"].astype(int, errors="ignore")
        for name, grp in df3.groupby("name"):
            eligible, off_p, cp_p = [], [], []
            for _, r in grp.iterrows():
                p = int(r["period"])
                subj = str(r["class"])
                if is_eligible(subj):
                    eligible.append(p)
                nsubj = normalize(subj)
                if "off" in nsubj:
                    off_p.append(p)
                if "career prep" in nsubj:
                    cp_p.append(p)
            
            target_dict = alternate_students if is_alternate(name) else regular_students
            
            if name not in target_dict:
                target_dict[name] = create_student_data()
            
            target_dict[name]["eligible_periods"] = sorted(set(target_dict[name]["eligible_periods"] + eligible))
            target_dict[name]["off_periods"] = sorted(set(target_dict[name]["off_periods"] + off_p))
            target_dict[name]["career_prep_periods"] = sorted(set(target_dict[name]["career_prep_periods"] + cp_p))

    return regular_students, alternate_students

def day_type_for(idx: int) -> str:
    # idx is the zero-based day counter in the generated schedule window
    return "A" if idx % 2 == 0 else "B"

def periods_for_daytype_with_filter(day_type: str, available_a_periods: set, available_b_periods: set):
    """Return only the periods that have eligible students for the given day type"""
    if day_type == "A":
        return sorted(available_a_periods)
    else:
        return sorted(available_b_periods)

def next_school_day(d: datetime, skip_weekends: bool) -> datetime:
    if not skip_weekends:
        return d + timedelta(days=1)
    # move to next day; if Sat, jump to Mon; if Sun, jump to Mon
    nd = d + timedelta(days=1)
    while nd.weekday() >= 5:  # 5=Sat, 6=Sun
        nd += timedelta(days=1)
    return nd

def calculate_fairness_metrics(schedule_rows: list, student_data: dict) -> dict:
    """Calculate comprehensive fairness metrics for the schedule"""
    # Handle empty schedule
    if not schedule_rows:
        return {
            "total_score": float('-inf'),
            "shift_std": 0,
            "shift_range": 0,
            "period_variance": 0,
            "consecutive_penalties": 0,
            "max_consecutive": 0,
            "weekday_variance": 0,
            "gap_fairness": 0
        }
    
    # Convert to DataFrame for easier analysis
    df = pd.DataFrame(schedule_rows)
    
    # Handle edge case with single or no assignments
    if df.empty or len(df) == 0:
        return {
            "total_score": float('-inf'),
            "shift_std": 0,
            "shift_range": 0,
            "period_variance": 0,
            "consecutive_penalties": 0,
            "max_consecutive": 0,
            "weekday_variance": 0,
            "gap_fairness": 0
        }
    
    # Basic shift distribution
    shift_counts = df.groupby('Student').size()
    shift_std = shift_counts.std() if len(shift_counts) > 1 else 0
    shift_range = shift_counts.max() - shift_counts.min() if len(shift_counts) > 1 else 0
    
    # Period distribution fairness
    period_distribution = {}
    for student in df['Student'].unique():
        student_periods = df[df['Student'] == student]['Period'].tolist()
        period_distribution[student] = defaultdict(int)
        for period in student_periods:
            period_distribution[student][period] += 1
    
    # Calculate period variance per student
    period_variances = []
    for student, periods in period_distribution.items():
        if len(periods) > 1:
            period_counts = list(periods.values())
            avg = sum(period_counts) / len(period_counts)
            variance = sum((x - avg)**2 for x in period_counts) / len(period_counts)
            period_variances.append(variance)
    
    period_variance_score = sum(period_variances) / len(period_variances) if period_variances else 0
    
    # Consecutive day penalties
    consecutive_penalties = 0
    max_consecutive_days = 0
    
    for student in df['Student'].unique():
        student_dates = pd.to_datetime(df[df['Student'] == student]['Date']).sort_values()
        if len(student_dates) > 1:
            consecutive_count = 1
            max_student_consecutive = 1
            
            for i in range(1, len(student_dates)):
                days_diff = (student_dates.iloc[i] - student_dates.iloc[i-1]).days
                if days_diff == 1:  # Consecutive days
                    consecutive_count += 1
                    consecutive_penalties += consecutive_count * 2  # Escalating penalty
                    max_student_consecutive = max(max_student_consecutive, consecutive_count)
                else:
                    consecutive_count = 1
            
            max_consecutive_days = max(max_consecutive_days, max_student_consecutive)
    
    # Day distribution fairness (spread across different days of week)
    df['Weekday'] = pd.to_datetime(df['Date']).dt.dayofweek
    weekday_distribution = {}
    for student in df['Student'].unique():
        student_weekdays = df[df['Student'] == student]['Weekday'].tolist()
        weekday_counts = defaultdict(int)
        for wd in student_weekdays:
            weekday_counts[wd] += 1
        weekday_distribution[student] = dict(weekday_counts)
    
    # Calculate weekday variance
    weekday_variances = []
    for student, weekdays in weekday_distribution.items():
        if len(weekdays) > 1:
            counts = list(weekdays.values())
            avg = sum(counts) / len(counts)
            variance = sum((x - avg)**2 for x in counts) / len(counts)
            weekday_variances.append(variance)
    
    weekday_variance_score = sum(weekday_variances) / len(weekday_variances) if weekday_variances else 0
    
    # Gap distribution (time between shifts)
    gap_fairness = 0
    for student in df['Student'].unique():
        student_dates = pd.to_datetime(df[df['Student'] == student]['Date']).sort_values()
        if len(student_dates) > 1:
            gaps = [(student_dates.iloc[i] - student_dates.iloc[i-1]).days for i in range(1, len(student_dates))]
            if gaps:
                gap_std = pd.Series(gaps).std()
                if not pd.isna(gap_std):  # Check for NaN
                    gap_fairness += gap_std
    
    # Calculate overall fairness score (lower is better, but we negate it)
    total_score = -(
        shift_std * 100 +                    # Heavy penalty for uneven shift distribution
        shift_range * 50 +                   # Penalty for range in shifts
        period_variance_score * 30 +         # Penalty for uneven period distribution
        consecutive_penalties * 20 +         # Penalty for consecutive days
        max_consecutive_days * 15 +          # Penalty for maximum consecutive streak
        weekday_variance_score * 25 +        # Penalty for uneven weekday distribution
        gap_fairness * 10                    # Penalty for irregular gaps
    )
    
    return {
        "total_score": total_score,
        "shift_std": shift_std,
        "shift_range": shift_range,
        "period_variance": period_variance_score,
        "consecutive_penalties": consecutive_penalties,
        "max_consecutive": max_consecutive_days,
        "weekday_variance": weekday_variance_score,
        "gap_fairness": gap_fairness
    }

def calculate_student_priority(name: str, period: int, current_date: datetime, 
                             student_data: dict, assigned_today: set, 
                             target_shifts: float, rng: random.Random) -> float:
    """Calculate priority score for assigning a student to a specific period"""
    data = student_data[name]
    
    # Skip if not eligible or already assigned today
    if period not in data["eligible_periods"] or name in assigned_today:
        return float('-inf')
    
    # Check minimum gap requirement (except for off periods)
    if data["last_worked"]:
        days_since = (current_date - data["last_worked"]).days
        if days_since < 1 and period not in data["off_periods"]:  # Minimum 1 day gap
            return float('-inf')
    
    # Calculate priority components
    shift_deficit = target_shifts - data["total_shifts"]
    period_balance = -data["period_counts"][period]  # Negative because fewer is better
    
    # Off period bonus
    off_period_bonus = 3.0 if period in data["off_periods"] else 0
    
    # Career prep bonus
    career_prep_bonus = 1.5 if period in data["career_prep_periods"] else 0
    
    # Recency score
    days_since_last = (current_date - data["last_worked"]).days if data["last_worked"] else 30
    recency_score = min(days_since_last / 3.0, 10.0)  # Cap at 10
    
    # Consecutive day penalty
    consecutive_penalty = 0
    if data["last_worked"] and (current_date - data["last_worked"]).days == 1:
        consecutive_penalty = -data["consecutive_days"] * 2.0
    
    # Recent period diversity (avoid same periods too frequently)
    recent_period_penalty = -data["recent_periods"].count(period) * 1.5
    
    # Weekday distribution bonus
    weekday = current_date.weekday()
    weekday_bonus = 0.5  # Small bonus for weekday variety (could be enhanced)
    
    # Random factor for tie-breaking
    random_factor = rng.random() * 0.3
    
    # Combine all factors
    priority_score = (
        shift_deficit * 10.0 +          # Highest priority: shift balance
        period_balance * 3.0 +          # Period distribution balance
        off_period_bonus +              # Strong preference for off periods
        career_prep_bonus +             # Moderate preference for career prep
        recency_score * 2.0 +           # Time since last shift
        consecutive_penalty +           # Penalty for consecutive days
        recent_period_penalty +         # Penalty for period repetition
        weekday_bonus +                 # Small weekday variety bonus
        random_factor                   # Tie breaker
    )
    
    return priority_score

def assign_students_to_period(period: int, current_date: datetime, day_type: str,
                            student_data: dict, assigned_today: set, 
                            max_per_period: int, target_shifts: float, 
                            rng: random.Random) -> list:
    """Assign students to a specific period using fair priority system"""
    
    # Calculate priorities for all students
    candidates = []
    for name in student_data:
        priority = calculate_student_priority(
            name, period, current_date, student_data, 
            assigned_today, target_shifts, rng
        )
        if priority > float('-inf'):
            candidates.append((name, priority))
    
    # Sort by priority (highest first)
    candidates.sort(key=lambda x: x[1], reverse=True)
    
    # Assign top candidates up to max_per_period
    assignments = []
    for i in range(min(len(candidates), max_per_period)):
        name = candidates[i][0]
        assignments.append(name)
        assigned_today.add(name)
        
        # Update student data
        data = student_data[name]
        data["last_worked"] = current_date
        data["total_shifts"] += 1
        data["period_counts"][period] += 1
        data["days_worked"].add(current_date.strftime("%Y-%m-%d"))
        
        # Update consecutive days tracking
        if data["last_worked"] and (current_date - data["last_worked"]).days == 1:
            data["consecutive_days"] += 1
        else:
            data["consecutive_days"] = 1
        
        data["max_consecutive"] = max(data["max_consecutive"], data["consecutive_days"])
        
        # Update recent periods (keep last 5)
        data["recent_periods"].append(period)
        if len(data["recent_periods"]) > 5:
            data["recent_periods"].pop(0)
    
    return assignments

def build_single_schedule(
    student_data: dict,
    start_date: datetime,
    num_days: int,
    available_a_periods: set,
    available_b_periods: set,
    max_per_period: int = 2,
    skip_weekends: bool = True,
    seed: int = 42
) -> list:
    """Build a single schedule iteration"""
    
    # Deep copy student data for this iteration
    current_student_data = deepcopy(student_data)
    
    # Initialize random generator for this iteration
    rng = random.Random(seed)
    
    # Calculate target shifts per student
    if not current_student_data:
        return []
    
    # Calculate actual available periods
    total_a_periods = len(available_a_periods)
    total_b_periods = len(available_b_periods)
    avg_periods_per_day = (total_a_periods + total_b_periods) / 2.0
    
    if avg_periods_per_day == 0:
        print("Warning: No available periods found!")
        return []
    
    total_slots = num_days * avg_periods_per_day * max_per_period
    target_shifts_per_student = total_slots / len(current_student_data)
    
    # Build schedule
    schedule_rows = []
    current_date = start_date
    days_scheduled = 0
    
    while days_scheduled < num_days:
        # Skip weekends if required
        if skip_weekends and current_date.weekday() >= 5:
            current_date += timedelta(days=(7 - current_date.weekday()))
        
        day_type = day_type_for(days_scheduled)
        assigned_today = set()
        
        # Get only available periods for this day type
        periods = periods_for_daytype_with_filter(day_type, available_a_periods, available_b_periods)
        
        # Schedule each available period for this day
        for period in periods:
            assignments = assign_students_to_period(
                period, current_date, day_type, current_student_data,
                assigned_today, max_per_period, target_shifts_per_student, rng
            )
            
            # Add assignments to schedule
            for student_name in assignments:
                schedule_rows.append({
                    "Date": current_date.strftime("%Y-%m-%d"),
                    "Day": day_type,
                    "Period": period,
                    "Student": student_name
                })
        
        days_scheduled += 1
        current_date = next_school_day(current_date, skip_weekends)
    
    return schedule_rows

def get_available_slots(regular_schedule: list, num_days: int, max_per_period: int,
                       available_a_periods: set, available_b_periods: set) -> dict:
    """Calculate which slots are available after regular students are scheduled"""
    # Create a mapping of date-period to current occupancy
    slot_occupancy = defaultdict(int)
    
    for entry in regular_schedule:
        slot_key = f"{entry['Date']}-{entry['Period']}"
        slot_occupancy[slot_key] += 1
    
    # Generate all possible slots
    available_slots = {}  # date-period -> available_count
    
    # We need to reconstruct the date sequence to know all possible slots
    if not regular_schedule:
        return available_slots
    
    # Find earliest date from regular schedule
    all_dates = [datetime.strptime(entry['Date'], '%Y-%m-%d') for entry in regular_schedule]
    start_date = min(all_dates)
    
    # Generate available slots for the same period
    current_date = start_date
    days_processed = 0
    
    while days_processed < num_days:
        if current_date.weekday() < 5:  # Weekdays only
            day_type = day_type_for(days_processed)
            
            # Use filtered periods based on availability
            periods = periods_for_daytype_with_filter(day_type, available_a_periods, available_b_periods)
            
            for period in periods:
                slot_key = f"{current_date.strftime('%Y-%m-%d')}-{period}"
                occupied = slot_occupancy[slot_key]
                available = max(0, max_per_period - occupied)
                
                if available > 0:
                    available_slots[slot_key] = {
                        'date': current_date.strftime('%Y-%m-%d'),
                        'period': period,
                        'day_type': day_type,
                        'available_count': available
                    }
            
            days_processed += 1
        
        current_date += timedelta(days=1)
    
    return available_slots

def build_alternate_schedule_for_slots(alternate_students: dict, available_slots: dict,
                                     seed: int, num_iterations: int = 20) -> list:
    """Build fair schedule for alternates using only available slots"""
    
    if not alternate_students or not available_slots:
        return []
    
    print(f"  Building alternate schedule for {len(available_slots)} available slots...")
    
    best_alternate_schedule = []
    best_alternate_score = float('-inf')
    
    # Calculate total available slots
    total_available = sum(slot['available_count'] for slot in available_slots.values())
    target_shifts_per_alternate = total_available / len(alternate_students) if alternate_students else 0
    
    print(f"    Total available slots: {total_available}, Target per alternate: {target_shifts_per_alternate:.1f}")
    
    for alt_iteration in range(num_iterations):
        current_alternates = deepcopy(alternate_students)
        alt_seed = seed + alt_iteration + 10000  # Ensure different seed space
        rng = random.Random(alt_seed)
        
        alternate_schedule = []
        assigned_today_alt = set()
        
        # Sort slots by date and period for consistent processing
        sorted_slots = sorted(available_slots.items(), 
                            key=lambda x: (x[1]['date'], x[1]['period']))
        
        for slot_key, slot_info in sorted_slots:
            slot_date = datetime.strptime(slot_info['date'], '%Y-%m-%d')
            period = slot_info['period']
            available_count = slot_info['available_count']
            
            # Reset daily assignments for new day
            current_day = slot_info['date']
            if not hasattr(build_alternate_schedule_for_slots, '_last_day') or \
               build_alternate_schedule_for_slots._last_day != current_day:
                assigned_today_alt.clear()
                build_alternate_schedule_for_slots._last_day = current_day
            
            # Assign alternates to this slot
            assignments = assign_students_to_period(
                period, slot_date, slot_info['day_type'], current_alternates,
                assigned_today_alt, available_count, target_shifts_per_alternate, rng
            )
            
            # Add assignments to schedule
            for student_name in assignments:
                alternate_schedule.append({
                    "Date": slot_info['date'],
                    "Day": slot_info['day_type'],
                    "Period": period,
                    "Student": student_name
                })
        
        # Evaluate this alternate iteration
        alt_metrics = calculate_fairness_metrics(alternate_schedule, alternate_students)
        
        if alt_metrics["total_score"] > best_alternate_score:
            best_alternate_score = alt_metrics["total_score"]
            best_alternate_schedule = alternate_schedule
    
    return best_alternate_schedule

def build_fair_schedule(
    regular_students: dict,
    alternate_students: dict,
    start_date: datetime,
    num_days: int,
    max_per_period: int = 2,
    skip_weekends: bool = True,
    seed: int = 42,
    num_iterations: int = 20
) -> pd.DataFrame:
    """Build the fairest possible schedule using multiple iterations"""
    
    print(f"\nBuilding schedule with {len(regular_students)} regular students and {len(alternate_students)} alternates...")
    
    # Scan for available periods across all students
    all_students = {**regular_students, **alternate_students}
    available_a_periods, available_b_periods = scan_available_periods(all_students)
    
    print(f"Available A-day periods: {sorted(available_a_periods) if available_a_periods else 'None'}")
    print(f"Available B-day periods: {sorted(available_b_periods) if available_b_periods else 'None'}")
    
    # Check if we have any valid periods
    if not available_a_periods and not available_b_periods:
        print("\nERROR: No eligible periods found for any students!")
        print("Please check that students have classes matching these keywords:")
        print(f"  {ELIGIBLE_KEYS}")
        return pd.DataFrame()
    
    print(f"Running {num_iterations} iterations to find the fairest schedule...")
    
    best_combined_schedule = []
    best_combined_score = float('-inf')
    best_metrics = {}
    
    # Build best regular schedule first
    best_regular_schedule = []
    best_regular_score = float('-inf')
    
    print("\nPhase 1: Optimizing regular student schedule...")
    for iteration in range(num_iterations):
        iteration_seed = seed + iteration
        print(f"  Regular iteration {iteration + 1}/{num_iterations}...")
        
        # Build schedule for regular students
        regular_schedule = build_single_schedule(
            regular_students, start_date, num_days, 
            available_a_periods, available_b_periods,
            max_per_period, skip_weekends, iteration_seed
        )
        
        # Evaluate regular schedule
        regular_metrics = calculate_fairness_metrics(regular_schedule, regular_students)
        
        if regular_metrics["total_score"] > best_regular_score:
            best_regular_score = regular_metrics["total_score"]
            best_regular_schedule = regular_schedule
            print(f"    New best regular score: {best_regular_score:.2f}")
    
    if not best_regular_schedule:
        print("\nWarning: No valid regular schedule could be created!")
        print("This might happen if:")
        print("  - No regular students have eligible periods")
        print("  - All constraints prevent any valid assignments")
    else:
        print(f"\nBest regular schedule found with score: {best_regular_score:.2f}")
    
    # Now build alternates schedule using the same fairness model
    if alternate_students:
        print("\nPhase 2: Optimizing alternate student schedule...")
        
        # Get available slots after regular students are scheduled
        available_slots = get_available_slots(
            best_regular_schedule, num_days, max_per_period,
            available_a_periods, available_b_periods
        )
        
        if available_slots:
            # Build fair alternate schedule
            best_alternate_schedule = build_alternate_schedule_for_slots(
                alternate_students, available_slots, seed, num_iterations
            )
            
            # Combine schedules
            best_combined_schedule = best_regular_schedule + best_alternate_schedule
            
            # Evaluate combined fairness
            combined_metrics = calculate_fairness_metrics(
                best_combined_schedule, {**regular_students, **alternate_students}
            )
            
            # Also get separate alternate metrics
            if best_alternate_schedule:
                alt_metrics = calculate_fairness_metrics(best_alternate_schedule, alternate_students)
                
                print(f"\nAlternate schedule metrics:")
                print(f"  Alternate fairness score: {alt_metrics['total_score']:.2f}")
                print(f"  Alternate shift std: {alt_metrics['shift_std']:.2f}")
                print(f"  Alternate shift range: {alt_metrics['shift_range']}")
            
            best_metrics = combined_metrics
            best_combined_score = combined_metrics["total_score"]
        else:
            print("  No available slots for alternates after regular scheduling")
            best_combined_schedule = best_regular_schedule
            best_metrics = calculate_fairness_metrics(best_regular_schedule, regular_students)
    else:
        best_combined_schedule = best_regular_schedule
        if best_combined_schedule:
            best_metrics = calculate_fairness_metrics(best_regular_schedule, regular_students)
        else:
            best_metrics = calculate_fairness_metrics([], {})
    
    # Print final metrics only if we have a valid schedule
    if best_combined_schedule and best_metrics["total_score"] != float('-inf'):
        print(f"\nFinal combined schedule metrics:")
        print(f"  Total fairness score: {best_metrics['total_score']:.2f}")
        print(f"  Shift standard deviation: {best_metrics['shift_std']:.2f}")
        print(f"  Shift range: {best_metrics['shift_range']}")
        print(f"  Period variance: {best_metrics['period_variance']:.2f}")
        print(f"  Consecutive penalties: {best_metrics['consecutive_penalties']}")
        print(f"  Max consecutive days: {best_metrics['max_consecutive']}")
    else:
        print("\nNo valid schedule could be created with the given constraints.")
    
    return pd.DataFrame(best_combined_schedule)

def main():
    ap = argparse.ArgumentParser(description="Fair CO-OP Shift Scheduler")
    ap.add_argument("--input", default="Employee_Schedule.xlsx", help="Path to Excel with either [Name, Schedule] or [Name, Period, Class]")
    ap.add_argument("--out", default="final_shift_schedule.xlsx", help="Output Excel filename")
    ap.add_argument("--csv", default=None, help="Optional CSV output filename")
    ap.add_argument("--start", required=False, default=None, help="Start date YYYY-MM-DD (defaults to first of current month)")
    ap.add_argument("--days", type=int, default=31, help="Number of school days to schedule")
    ap.add_argument("--max-per-period", type=int, default=2, help="Max students per period")
    ap.add_argument("--skip-weekends", action="store_true", help="Skip Saturdays/Sundays")
    ap.add_argument("--seed", type=int, default=42, help="Random seed for tie-breakers")
    ap.add_argument("--iterations", type=int, default=20, help="Number of iterations to find fairest schedule")
    args = ap.parse_args()

    if args.start:
        start_date = datetime.strptime(args.start, "%Y-%m-%d")
    else:
        today = datetime.today()
        start_date = datetime(today.year, today.month, 1)

    # Load Excel (first sheet)
    try:
        df = pd.read_excel(args.input)
    except Exception as e:
        print(f"Error loading input file: {e}")
        return
    
    # Load student periods
    try:
        regular_students, alternate_students = load_student_periods(df)
    except Exception as e:
        print(f"Error parsing student data: {e}")
        print("Please ensure your spreadsheet has either:")
        print("  - Columns [Name, Schedule] where Schedule is like '1 - Math, 2 - Off, ...'")
        print("  - Columns [Name, Period, Class]")
        return
    
    print(f"\nLoaded {len(regular_students)} regular students and {len(alternate_students)} alternates")
    
    # Perform sanity checks
    all_students = {**regular_students, **alternate_students}
    if not all_students:
        print("\nERROR: No students found in the spreadsheet!")
        return
    
    # Check if any students have eligible periods
    total_eligible_periods = sum(len(s['eligible_periods']) for s in all_students.values())
    if total_eligible_periods == 0:
        print("\nERROR: No students have eligible periods!")
        print("Eligible subjects must contain one of these keywords:")
        for keyword in ELIGIBLE_KEYS:
            print(f"  - {keyword}")
        return
    
    # Show which students have which eligible periods (for debugging)
    print("\nEligible periods by student:")
    for name, data in sorted(all_students.items())[:5]:  # Show first 5 for brevity
        if data['eligible_periods']:
            student_type = "ALT" if is_alternate(name) else "REG"
            periods_str = ", ".join(map(str, data['eligible_periods']))
            print(f"  {name} ({student_type}): periods {periods_str}")
    if len(all_students) > 5:
        print(f"  ... and {len(all_students) - 5} more students")

    # Build the schedule
    schedule_df = build_fair_schedule(
        regular_students=regular_students,
        alternate_students=alternate_students,
        start_date=start_date,
        num_days=args.days,
        max_per_period=args.max_per_period,
        skip_weekends=args.skip_weekends,
        seed=args.seed,
        num_iterations=args.iterations
    )

    # Check if we got a valid schedule
    if schedule_df.empty:
        print("\nNo valid schedule could be generated. Please check:")
        print("  1. Students have eligible periods (containing 'off', 'career prep', 'management', etc.)")
        print("  2. The constraints (max per period, days, etc.) allow for valid assignments")
        print("  3. The input data is correctly formatted")
        return

    # Create detailed summary with fairness metrics
    summary = schedule_df.groupby("Student").agg({
        'Date': 'count',
        'Period': lambda x: list(x)
    }).rename(columns={'Date': 'Total_Shifts', 'Period': 'Periods_Worked'})
    
    summary['Period_Distribution'] = summary['Periods_Worked'].apply(
        lambda periods: {p: periods.count(p) for p in set(periods)}
    )
    summary['Unique_Periods'] = summary['Periods_Worked'].apply(lambda x: len(set(x)))
    summary = summary.drop('Periods_Worked', axis=1)
    summary = summary.reset_index().sort_values("Total_Shifts", ascending=False)
    
    # Add student type indicator
    summary['Type'] = summary['Student'].apply(
        lambda x: 'Alternate' if is_alternate(x) else 'Regular'
    )

    # Save results
    try:
        with pd.ExcelWriter(args.out, engine="openpyxl") as writer:
            schedule_df.to_excel(writer, index=False, sheet_name="Schedule")
            if not summary.empty:
                summary.to_excel(writer, index=False, sheet_name="Summary")
        print(f"\nSuccessfully wrote schedule to {args.out}")
    except Exception as e:
        print(f"\nError saving Excel file: {e}")
        # Try to save as CSV as backup
        try:
            backup_csv = args.out.replace('.xlsx', '_backup.csv')
            schedule_df.to_csv(backup_csv, index=False)
            print(f"Saved backup to {backup_csv}")
        except:
            pass

    if args.csv:
        try:
            schedule_df.to_csv(args.csv, index=False)
            print(f"Wrote CSV to {args.csv}")
        except Exception as e:
            print(f"Error saving CSV file: {e}")
    
    # Print final statistics
    print(f"\nSchedule Statistics:")
    print(f"  Total assignments: {len(schedule_df)}")
    print(f"  Date range: {schedule_df['Date'].min()} to {schedule_df['Date'].max()}")
    print(f"  Unique students scheduled: {schedule_df['Student'].nunique()}")
    
    # Show assignment distribution
    print(f"\nAssignments per student:")
    student_counts = schedule_df.groupby('Student').size().sort_values(ascending=False)
    
    # Separate regular and alternate students
    regular_counts = []
    alternate_counts = []
    
    for student, count in student_counts.items():
        if is_alternate(student):
            alternate_counts.append((student, count))
        else:
            regular_counts.append((student, count))
    
    # Show regular students
    if regular_counts:
        print("  Regular students:")
        for student, count in regular_counts[:10]:  # Show top 10
            print(f"    {student}: {count} shifts")
        if len(regular_counts) > 10:
            print(f"    ... and {len(regular_counts) - 10} more regular students")
    
    # Show alternate students
    if alternate_counts:
        print("  Alternate students:")
        for student, count in alternate_counts[:10]:  # Show top 10
            print(f"    {student}: {count} shifts")
        if len(alternate_counts) > 10:
            print(f"    ... and {len(alternate_counts) - 10} more alternate students")
    
    # Show period distribution
    print(f"\nPeriod distribution:")
    period_counts = schedule_df['Period'].value_counts().sort_index()
    for period, count in period_counts.items():
        print(f"  Period {period}: {count} assignments")

if __name__ == "__main__":
    main()
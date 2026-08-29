import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os
import random
from typing import Dict, List, Tuple, Optional
import holidays
import logging

logger = logging.getLogger(__name__)


class RealisticSalesDataGenerator:
    """Generate realistic sales data with multiple files, partitions, and Egyptian business patterns"""
    
    def __init__(self, start_date: str = "2023-01-01", end_date: str = "2025-12-31"):
        self.start_date = pd.to_datetime(start_date)
        self.end_date = pd.to_datetime(end_date)
        
        # Egyptian Public Holidays
        self.eg_holidays = holidays.Egypt(years=list(range(self.start_date.year, self.end_date.year + 1)))
        
        # Store configurations (Egyptian Cities & Retail Hubs)
        self.stores = {
            'store_001': {'location': 'Cairo - Nasr City', 'size': 'large', 'base_traffic': 1500},
            'store_002': {'location': 'Giza - Sheikh Zayed', 'size': 'large', 'base_traffic': 1200},
            'store_003': {'location': 'Alexandria - San Stefano', 'size': 'large', 'base_traffic': 1100},
            'store_004': {'location': 'Mansoura', 'size': 'medium', 'base_traffic': 800},
            'store_005': {'location': 'Tanta', 'size': 'medium', 'base_traffic': 750},
            'store_006': {'location': 'Sharm El Sheikh', 'size': 'medium', 'base_traffic': 600},
            'store_007': {'location': 'Hurghada', 'size': 'medium', 'base_traffic': 650},
            'store_008': {'location': 'Port Said', 'size': 'small', 'base_traffic': 500},
            'store_009': {'location': 'Asyut', 'size': 'medium', 'base_traffic': 700},
            'store_010': {'location': 'Zagazig', 'size': 'small', 'base_traffic': 450}
        }
        
        # Product categories and items (Prices in EGP)
        self.product_categories = {
            'Electronics': {
                'ELEC_001': {'name': 'Smartphone', 'price': 18000, 'margin': 0.12, 'seasonality': 'white_friday'},
                'ELEC_002': {'name': 'Laptop', 'price': 32000, 'margin': 0.10, 'seasonality': 'back_to_school'},
                'ELEC_003': {'name': 'Headphones', 'price': 2500, 'margin': 0.22, 'seasonality': 'white_friday'},
                'ELEC_004': {'name': 'Tablet', 'price': 12000, 'margin': 0.15, 'seasonality': 'back_to_school'},
                'ELEC_005': {'name': 'Smart Watch', 'price': 6500, 'margin': 0.18, 'seasonality': 'eid'}
            },
            'Clothing': {
                'CLTH_001': {'name': 'T-Shirt', 'price': 450, 'margin': 0.45, 'seasonality': 'summer'},
                'CLTH_002': {'name': 'Jeans', 'price': 950, 'margin': 0.40, 'seasonality': 'eid'},
                'CLTH_003': {'name': 'Jacket', 'price': 2200, 'margin': 0.38, 'seasonality': 'winter'},
                'CLTH_004': {'name': 'Dress', 'price': 1300, 'margin': 0.45, 'seasonality': 'eid'},
                'CLTH_005': {'name': 'Shoes', 'price': 1600, 'margin': 0.35, 'seasonality': 'eid'}
            },
            'Home & Appliances': {
                'HOME_001': {'name': 'Air Conditioner', 'price': 24000, 'margin': 0.18, 'seasonality': 'summer'},
                'HOME_002': {'name': 'Blender', 'price': 1800, 'margin': 0.30, 'seasonality': 'ramadan'},
                'HOME_003': {'name': 'Air Fryer', 'price': 5500, 'margin': 0.25, 'seasonality': 'ramadan'},
                'HOME_004': {'name': 'Vacuum Cleaner', 'price': 4200, 'margin': 0.22, 'seasonality': 'all_year'},
                'HOME_005': {'name': 'Microwave', 'price': 4800, 'margin': 0.20, 'seasonality': 'ramadan'}
            },
            'Sports & Outdoor': {
                'SPRT_001': {'name': 'Yoga Mat', 'price': 600, 'margin': 0.50, 'seasonality': 'fitness'},
                'SPRT_002': {'name': 'Dumbbells Set', 'price': 1500, 'margin': 0.40, 'seasonality': 'fitness'},
                'SPRT_003': {'name': 'Running Shoes', 'price': 2800, 'margin': 0.30, 'seasonality': 'spring'},
                'SPRT_004': {'name': 'Bicycle', 'price': 8500, 'margin': 0.20, 'seasonality': 'summer'},
                'SPRT_005': {'name': 'Football', 'price': 800, 'margin': 0.45, 'seasonality': 'summer'}
            }
        }
        
        # Flatten products
        self.all_products = {}
        for category, products in self.product_categories.items():
            for product_id, product_info in products.items():
                self.all_products[product_id] = {**product_info, 'category': category}
    
    def get_seasonality_factor(self, date: pd.Timestamp, seasonality_type: str) -> float:
        """Calculate seasonality factor aligned with Egyptian commercial cycles"""
        day_of_year = date.dayofyear
        
        if seasonality_type == 'white_friday':
            # Peak during November White Friday season
            if date.month == 11:
                return 1.8 + 0.4 * np.sin(2 * np.pi * (day_of_year - 300) / 30)
            return 1.0
            
        elif seasonality_type == 'ramadan' or seasonality_type == 'eid':
            # High spike around Eid / Ramadan buying sprees (Approximate seasonal bump around spring/early summer)
            if date.month in [3, 4, 5]:
                return 1.6
            return 1.0
        
        elif seasonality_type == 'summer':
            # Peak June-August (Sahel/Beach season & High clothing sales)
            return 1.0 + 0.5 * np.sin(2 * np.pi * (day_of_year - 100) / 365)
        
        elif seasonality_type == 'winter':
            # Peak November-February
            return 1.0 + 0.4 * np.sin(2 * np.pi * (day_of_year + 80) / 365)
        
        elif seasonality_type == 'back_to_school':
            # Egyptian school season starts mid-September/October
            if date.month in [9, 10]:
                return 1.6
            return 0.85
        
        elif seasonality_type == 'fitness':
            # Post-New Year & Early Summer preparation
            if date.month in [1, 5]:
                return 1.4
            return 1.0
        
        elif seasonality_type == 'spring':
            # March to May (Sham El-Nessim)
            return 1.0 + 0.25 * np.sin(2 * np.pi * (day_of_year - 30) / 365)
        
        else:  # all_year
            return 1.0
    
    def get_day_of_week_factor(self, date: pd.Timestamp) -> float:
        """Get multiplier based on Egyptian Weekend (Friday/Saturday high traffic)"""
        dow = date.dayofweek
        # Monday=0, Tuesday=1, Wednesday=2, Thursday=3, Friday=4, Saturday=5, Sunday=6
        # Weekend in Egypt: Friday (4) and Saturday (5), Thursday evening (3) is also high
        dow_factors = [0.85, 0.85, 0.90, 1.15, 1.45, 1.35, 0.95]
        return dow_factors[dow]
    
    def generate_promotions(self) -> pd.DataFrame:
        """Generate promotional calendar for Egyptian market events"""
        promotions = []
        
        # Egyptian Commercial Events
        major_events = [
            ('White Friday', 11, 20, 10, 0.30),      # November White Friday season
            ('Eid El-Fitr Sale', 4, 10, 7, 0.20),     # End of Ramadan shopping
            ('Eid El-Adha Sale', 6, 15, 7, 0.20),     # Summer Eid sale
            ('Back to School', 9, 1, 20, 0.15),       # September school prep
            ('New Year Sale', 12, 25, 7, 0.25),       # End of year clearance
            ('Mother Day Sale', 3, 15, 7, 0.20),      # March 21st Mother's Day
            ('Summer Clearance', 8, 15, 10, 0.25)
        ]
        
        current_date = self.start_date
        while current_date <= self.end_date:
            year = current_date.year
            
            for event_name, month, day, duration, discount in major_events:
                try:
                    event_date = pd.Timestamp(year, month, day)
                except:
                    continue
                
                if self.start_date <= event_date <= self.end_date:
                    for d in range(duration):
                        promo_date = event_date + timedelta(days=d)
                        if promo_date <= self.end_date:
                            promo_products = random.sample(
                                list(self.all_products.keys()), 
                                k=random.randint(5, 12)
                            )
                            for product_id in promo_products:
                                promotions.append({
                                    'date': promo_date,
                                    'product_id': product_id,
                                    'promotion_type': event_name,
                                    'discount_percent': discount
                                })
            
            current_date = current_date + pd.DateOffset(years=1)
            
        # Flash sales
        n_flash_sales = int((self.end_date - self.start_date).days * 0.04)
        flash_dates = pd.date_range(self.start_date, self.end_date, periods=n_flash_sales)
        
        for date in flash_dates:
            promo_products = random.sample(list(self.all_products.keys()), k=random.randint(3, 6))
            for product_id in promo_products:
                promotions.append({
                    'date': date,
                    'product_id': product_id,
                    'promotion_type': 'Flash Sale',
                    'discount_percent': round(random.uniform(0.10, 0.25), 2)
                })
        
        return pd.DataFrame(promotions)
    
    def generate_store_events(self) -> pd.DataFrame:
        """Generate store closures/renovations"""
        events = []
        for store_id in self.stores.keys():
            n_closures = random.randint(1, 3)
            closure_dates = pd.date_range(self.start_date, self.end_date, periods=n_closures)
            
            for date in closure_dates:
                events.append({
                    'store_id': store_id,
                    'date': date,
                    'event_type': 'closure',
                    'impact': -1.0
                })
        return pd.DataFrame(events)

    def generate_sales_data(self, output_dir: str = "/tmp/egypt_sales_data") -> Dict[str, List[str]]:
        """Generate sales data partitioned by date and store"""
        os.makedirs(output_dir, exist_ok=True)
        
        promotions_df = self.generate_promotions()
        store_events_df = self.generate_store_events()
        
        file_paths = {
            'sales': [],
            'inventory': [],
            'customer_traffic': [],
            'promotions': [],
            'store_events': []
        }
        
        # Save supplementary data
        promotions_path = os.path.join(output_dir, "promotions/promotions.parquet")
        os.makedirs(os.path.dirname(promotions_path), exist_ok=True)
        promotions_df.to_parquet(promotions_path, index=False)
        file_paths['promotions'].append(promotions_path)
        
        events_path = os.path.join(output_dir, "store_events/events.parquet")
        os.makedirs(os.path.dirname(events_path), exist_ok=True)
        store_events_df.to_parquet(events_path, index=False)
        file_paths['store_events'].append(events_path)
        
        current_date = self.start_date
        
        while current_date <= self.end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            
            daily_sales_data = []
            daily_traffic_data = []
            daily_inventory_data = []
            
            for store_id, store_info in self.stores.items():
                base_traffic = store_info['base_traffic']
                dow_factor = self.get_day_of_week_factor(current_date)
                is_holiday = current_date in self.eg_holidays
                holiday_factor = 1.35 if is_holiday else 1.0
                
                weather_factor = max(0.6, min(1.1, np.random.normal(1.0, 0.08)))
                
                store_event_impact = 1.0
                if not store_events_df.empty:
                    event = store_events_df[
                        (store_events_df['store_id'] == store_id) & 
                        (store_events_df['date'] == current_date)
                    ]
                    if not event.empty:
                        store_event_impact = 1.0 + event.iloc[0]['impact']
                
                store_traffic = int(
                    base_traffic * dow_factor * holiday_factor * 
                    weather_factor * store_event_impact * 
                    np.random.normal(1.0, 0.05)
                )
                
                daily_traffic_data.append({
                    'date': current_date,
                    'store_id': store_id,
                    'customer_traffic': max(0, store_traffic),
                    'weather_impact': weather_factor,
                    'is_holiday': is_holiday
                })
                
                for product_id, product_info in self.all_products.items():
                    seasonality_factor = self.get_seasonality_factor(
                        current_date, product_info['seasonality']
                    )
                    
                    discount_percent = 0.0
                    promotion_factor = 1.0
                    if not promotions_df.empty:
                        promo = promotions_df[
                            (promotions_df['date'] == current_date) & 
                            (promotions_df['product_id'] == product_id)
                        ]
                        if not promo.empty:
                            discount_percent = promo.iloc[0]['discount_percent']
                            promotion_factor = 1.0 + (discount_percent * 2.5)
                    
                    size_factor = {'large': 1.0, 'medium': 0.7, 'small': 0.45}[store_info['size']]
                    price_factor = 1.0 / (1.0 + product_info['price'] / 5000)
                    
                    base_quantity = store_traffic * 0.002 * size_factor * price_factor
                    
                    quantity = int(
                        base_quantity * seasonality_factor * promotion_factor *
                        np.random.normal(1.0, 0.2)
                    )
                    quantity = max(0, quantity)
                    
                    actual_price = product_info['price'] * (1 - discount_percent)
                    revenue = quantity * actual_price
                    cost = quantity * product_info['price'] * (1 - product_info['margin'])
                    
                    if quantity > 0:
                        daily_sales_data.append({
                            'date': current_date,
                            'store_id': store_id,
                            'product_id': product_id,
                            'category': product_info['category'],
                            'quantity_sold': quantity,
                            'unit_price_egp': product_info['price'],
                            'discount_percent': discount_percent,
                            'revenue_egp': revenue,
                            'cost_egp': cost,
                            'profit_egp': revenue - cost
                        })
                    
                    inventory_level = random.randint(30, 150)
                    reorder_point = random.randint(15, 35)
                    
                    daily_inventory_data.append({
                        'date': current_date,
                        'store_id': store_id,
                        'product_id': product_id,
                        'inventory_level': inventory_level,
                        'reorder_point': reorder_point,
                        'days_of_supply': inventory_level / max(1, quantity)
                    })
            
            # Save files
            if daily_sales_data:
                sales_df = pd.DataFrame(daily_sales_data)
                sales_path = os.path.join(
                    output_dir, 
                    f"sales/year={current_date.year}/month={current_date.month:02d}/day={current_date.day:02d}/"
                    f"sales_{date_str}.parquet"
                )
                os.makedirs(os.path.dirname(sales_path), exist_ok=True)
                sales_df.to_parquet(sales_path, index=False)
                file_paths['sales'].append(sales_path)
            
            if daily_traffic_data:
                traffic_df = pd.DataFrame(daily_traffic_data)
                traffic_path = os.path.join(
                    output_dir,
                    f"customer_traffic/year={current_date.year}/month={current_date.month:02d}/day={current_date.day:02d}/"
                    f"traffic_{date_str}.parquet"
                )
                os.makedirs(os.path.dirname(traffic_path), exist_ok=True)
                traffic_df.to_parquet(traffic_path, index=False)
                file_paths['customer_traffic'].append(traffic_path)
            
            # Weekly inventory snapshot on Saturdays
            if daily_inventory_data and current_date.dayofweek == 5:
                inventory_df = pd.DataFrame(daily_inventory_data)
                inventory_path = os.path.join(
                    output_dir,
                    f"inventory/year={current_date.year}/week={current_date.isocalendar()[1]:02d}/"
                    f"inventory_{date_str}.parquet"
                )
                os.makedirs(os.path.dirname(inventory_path), exist_ok=True)
                inventory_df.to_parquet(inventory_path, index=False)
                file_paths['inventory'].append(inventory_path)
            
            current_date += timedelta(days=1)
        metadata = {
            'generation_date': datetime.now().isoformat(),
            'start_date': self.start_date.isoformat(),
            'end_date': self.end_date.isoformat(),
            'n_stores': len(self.stores),
            'n_products': len(self.all_products),
            'file_counts': {k: len(v) for k, v in file_paths.items()},
            'total_files': sum(len(v) for v in file_paths.values())
        }
        metadata_df = pd.DataFrame([metadata])
        metadata_path = os.path.join(output_dir, "metadata/generation_metadata.parquet")
        os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
        metadata_df.to_parquet(metadata_path, index=False)
        
        logger.info(f"Generated {metadata['total_files']} files")
        logger.info(f"Sales files: {len(file_paths['sales'])}")
        logger.info(f"Customer traffic files: {len(file_paths['customer_traffic'])}")
        logger.info(f"Inventory files: {len(file_paths['inventory'])}")
        logger.info(f"Output directory: {output_dir}")
            
        return file_paths
    
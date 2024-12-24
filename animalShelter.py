from pymongo import MongoClient
from bson.objectid import ObjectId
from bson.json_util import dumps
import logging
import pandas as pd

class AnimalShelter(object):
    """ CRUD operations for Animal collection in MongoDB """

    def __init__(self, username, password, host='localhost', port=39329, db='AAC'):
        """
        Initialize the MongoClient with error handling
        
        Parameters:
            username (str): MongoDB username
            password (str): MongoDB password
            host (str): MongoDB host address
            port (int): MongoDB port number
            db (str): Database name
        """
        try:
            self.client = MongoClient(f'mongodb://{username}:{password}@{host}:{port}/{db}?authSource=AAC')
            self.database = self.client[db]
            
            # Test connection
            self.client.server_info()
            logging.info("Connected to MongoDB successfully")
            
        except Exception as e:
            logging.error("Failed to connect to MongoDB")
            logging.error(e)
            raise

    def create(self, data):
        """
        Create a new animal record
        
        Parameters:
            data (dict): Animal data to insert
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not data or not isinstance(data, dict):
                raise ValueError("Invalid data format: expected dictionary")
            
            # Validate required fields
            required_fields = ['animal_type', 'breed', 'age_upon_outcome_in_weeks']
            if not all(field in data for field in required_fields):
                raise ValueError(f"Missing required fields. Required: {required_fields}")
            
            insert_result = self.database.animals.insert_one(data)
            return bool(insert_result.inserted_id)
            
        except Exception as e:
            logging.error(f"Error creating record: {e}")
            return False

    def read(self, criteria=None, projection=None):
        """
        Read animal records with enhanced filtering
        
        Parameters:
            criteria (dict): Query criteria
            projection (dict): Fields to include/exclude
            
        Returns:
            list: Matching documents
        """
        try:
            if criteria is None:
                criteria = {}
                
            if not isinstance(criteria, dict):
                raise ValueError("Criteria must be a dictionary")
                
            # Default projection to exclude _id
            if projection is None:
                projection = {'_id': False}
                
            cursor = self.database.animals.find(criteria, projection)
            return list(cursor)
            
        except Exception as e:
            logging.error(f"Error reading records: {e}")
            return []

    def update(self, criteria, update_data):
        """
        Update animal records
        
        Parameters:
            criteria (dict): Query criteria for records to update
            update_data (dict): Data to update
            
        Returns:
            dict: Update operation result
        """
        try:
            if not criteria or not update_data:
                raise ValueError("Both criteria and update_data are required")
                
            if not isinstance(criteria, dict) or not isinstance(update_data, dict):
                raise ValueError("Both criteria and update_data must be dictionaries")
            
            # Ensure update operator is present
            if not any(key.startswith('$') for key in update_data.keys()):
                update_data = {'$set': update_data}
            
            result = self.database.animals.update_many(criteria, update_data)
            
            return {
                'matched_count': result.matched_count,
                'modified_count': result.modified_count,
                'success': result.modified_count > 0
            }
            
        except Exception as e:
            logging.error(f"Error updating records: {e}")
            return {'success': False, 'error': str(e)}

    def delete(self, criteria):
        """
        Delete animal records
        
        Parameters:
            criteria (dict): Query criteria for records to delete
            
        Returns:
            dict: Delete operation result
        """
        try:
            if not criteria:
                raise ValueError("Delete criteria cannot be empty")
                
            if not isinstance(criteria, dict):
                raise ValueError("Criteria must be a dictionary")
            
            result = self.database.animals.delete_many(criteria)
            
            return {
                'deleted_count': result.deleted_count,
                'success': result.deleted_count > 0
            }
            
        except Exception as e:
            logging.error(f"Error deleting records: {e}")
            return {'success': False, 'error': str(e)}

    def get_breeds_by_rescue_type(self, rescue_type):
        """
        Get available breeds for specific rescue types
        
        Parameters:
            rescue_type (str): Type of rescue ('water', 'mount', 'disaster')
            
        Returns:
            list: Matching breed records
        """
        rescue_criteria = {
            'water': {
                "animal_type": "Dog",
                "breed": {"$in": ["Labrador Retriever Mix", "Chesapeake Bay Retriever", "Newfoundland"]},
                "sex_upon_outcome": "Intact Female",
                "age_upon_outcome_in_weeks": {"$gte": 26.0, "$lte": 156.0}
            },
            'mount': {
                "animal_type": "Dog",
                "breed": {"$in": ["German Shepherd", "Alaskan Malamute", "Old English Sheepdog", 
                                 "Siberian Husky", "Rottweiler"]},
                "sex_upon_outcome": "Intact Male",
                "age_upon_outcome_in_weeks": {"$gte": 26.0, "$lte": 156.0}
            },
            'disaster': {
                "animal_type": "Dog",
                "breed": {"$in": ["Doberman Pinscher", "German Shepherd", "Golden Retriever", 
                                 "Bloodhound", "Rottweiler"]},
                "sex_upon_outcome": "Intact Male",
                "age_upon_outcome_in_weeks": {"$gte": 20.0, "$lte": 300.0}
            }
        }
        
        if rescue_type not in rescue_criteria:
            return []
            
        return self.read(rescue_criteria[rescue_type])

    def get_animal_statistics(self):
        """
        Get statistics about the animal shelter data
        
        Returns:
            dict: Various statistics about the animals
        """
        try:
            pipeline = [
                {
                    '$group': {
                        '_id': None,
                        'total_animals': {'$sum': 1},
                        'avg_age_weeks': {'$avg': '$age_upon_outcome_in_weeks'},
                        'breeds': {'$addToSet': '$breed'}
                    }
                }
            ]
            
            stats = list(self.database.animals.aggregate(pipeline))[0]
            stats['unique_breeds'] = len(stats['breeds'])
            del stats['_id']
            del stats['breeds']
            
            return stats
            
        except Exception as e:
            logging.error(f"Error getting statistics: {e}")
            return {}

    def __str__(self):
        """String representation of the database status"""
        try:
            stats = self.get_animal_statistics()
            return f"Animal Shelter Database - Total Animals: {stats.get('total_animals', 0)}"
        except:
            return "Animal Shelter Database - Status Unknown"

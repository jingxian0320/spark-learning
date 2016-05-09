from __future__ import print_function
import numpy as np
import math
import itertools
import random
from pyspark import SparkContext
from pyspark.mllib.recommendation import ALS, Rating

def compute_rmse(model, data):
    '''
    Computer RMSE of the model.
    '''
    testdata = data.map(lambda p: (p[0], p[1]))
    predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
    ratesAndPreds = data.map(lambda r: ((r[0], r[1]), r[2])).join(predictions)
    rmse = math.sqrt(ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean())
    return rmse

def train_model_with_preset_param(ratings):
    '''
    Train with preset parameters.
    '''
    # Build the recommendation model using Alternating Least Squares
    rank = 12
    numIterations = 20
    model = ALS.trainImplicit(ratings, rank, numIterations)

    # Evaluate the model on training data
    RMSE = compute_rmse(model, ratings)
    print("Root Mean Squared Error = " + str(RMSE))

    return model

def train_model_with_validation(ratings):
    '''
    Train and select the best model with lowest rmse.
    '''
    ranks = [10, 12]
    lambdas = [0.01, 0.1, 1.0]
    numIters = [10, 20]
    bestModel = None
    bestValidationRmse = float("inf")
    bestRank = 0
    bestLambda = -1.0
    bestNumIter = -1

    for rank, lmbda, numIter in itertools.product(ranks, lambdas, numIters):
        # Build the recommendation model using Alternating Least Squares
        model = ALS.trainImplicit(ratings, rank, numIter, lmbda)

        # Evaluate the model on validation data
        validationRmse = compute_rmse(model, ratings)
        print("RMSE = %f for the model trained with rank = %d, lambda = %.2f, and numIter = %d."
            % (validationRmse, rank, lmbda, numIter))

        if validationRmse < bestValidationRmse :
            bestModel = model
            bestValidationRmse = validationRmse
            bestRank = rank
            bestLambda = lmbda
            bestNumIter = numIter

    print("The best model was trained with rank = %d, lambda = %.2f, numIter = %d, and its RMSE is %f."
        % (bestRank, bestLambda, bestNumIter, bestValidationRmse))

    return bestModel


def train_model_with_validation_test(ratings):
    '''
    Divide ratings into training, validation and test sets. 
    Train and select the best model with lowest rmse on validation set. 
    '''
    training, validation, test = ratings.randomSplit([6, 2, 2], seed=0L)
    print("Training: %d, validation: %d, test: %d" % (training.count(), validation.count(), test.count()))

    ranks = [10, 12]
    lambdas = [0.01, 0.1, 1.0]
    numIters = [10, 20]
    bestModel = None
    bestValidationRmse = float("inf")
    bestRank = 0
    bestLambda = -1.0
    bestNumIter = -1

    for rank, lmbda, numIter in itertools.product(ranks, lambdas, numIters):
        # Build the recommendation model using Alternating Least Squares
        model = ALS.trainImplicit(training, rank, numIter, lmbda)

        # Evaluate the model on validation data
        validationRmse = compute_rmse(model, validation)
        print("RMSE (validation) = %f for the model trained with rank = %d, lambda = %.2f, and numIter = %d."
            % (validationRmse, rank, lmbda, numIter))

        if validationRmse < bestValidationRmse :
            bestModel = model
            bestValidationRmse = validationRmse
            bestRank = rank
            bestLambda = lmbda
            bestNumIter = numIter

    # Evaluate the best model on the test set
    testRmse = compute_rmse(bestModel, test)
    print("The best model was trained with rank = %d, lambda = %.2f, numIter = %d, and its RMSE on the test set is %f."
        % (bestRank, bestLambda, bestNumIter, testRmse))

    return bestModel


def sampleInteractions(user_id,items_with_rating,n):
    '''
    For users with # interactions > n, replace their interaction history
    with a sample of n items_with_rating
    '''
    if len(items_with_rating) > n:
        return user_id, random.sample(items_with_rating,n)
    else:
        return user_id, items_with_rating

def itemPairs((user_id,items_with_rating)):
    '''
    For each user, find all item-item pairs.
    '''
    combis = itertools.permutations(items_with_rating, 2)

    # Generate the combinations
    def gen(c):
        while True:
            (left_y, left_r), (right_y, right_r) = c.next()
            yield ((left_y, right_y), (left_r, right_r))
    
    return gen(combis)
    
def toStruct( ((left_y, right_y), (left_r, right_r)) ):
    left_square, right_square = left_r ** 2, right_r ** 2
    r_product = left_r * right_r
    return (
            (left_y, right_y),
            (r_product, left_square, right_square, 1)
            )

def structReducer(
                   (a_sum_prod, a_left_sum_square, a_right_sum_square, a_count),
                   (b_sum_prod, b_left_sum_square, b_right_sum_square, b_count)
                   ):
    # Reduce the (item, item) (product_of_ratings, left_items_rating_squared, right_items_rating_squared) pairs
    # into (item, item) (SUM(product_of_ratings), SUM(left_items_rating_squared), SUM(right_items_rating_squared), count) pairs
    return (
            a_sum_prod + b_sum_prod,
            a_left_sum_square + b_left_sum_square,
            a_right_sum_square + b_right_sum_square,
            a_count + b_count
            )
            
def calcSim((item_pair,(dot_product,rating_norm_squared,rating2_norm_squared,count))):
    ''' 
    For each item-item pair, return the specified similarity measure,
    along with co_raters_count
    '''
    cos_sim = cosine(dot_product,np.sqrt(rating_norm_squared),np.sqrt(rating2_norm_squared))
    return item_pair, (cos_sim,count)

def cosine(dot_product,rating_norm,rating2_norm):
    '''
    The cosine between two vectors A, B
       dotProduct(A, B) / (norm(A) * norm(B))
    '''
    denominator = rating_norm* rating2_norm
    return (dot_product / (float(denominator))) if denominator else 0.0

def keyOnFirstItem(item_pair,item_sim_data):
    '''
    For each item-item pair, make the first item's id the key
    '''
    (item1_id,item2_id) = item_pair
    return item1_id,(item2_id,item_sim_data)

def nearestNeighbors(item_id,items_and_sims,n):
    '''
    Sort the predictions list by similarity and select the top-N neighbors
    '''
    items_and_sims.sort(key=lambda x: x[1][0],reverse=True)
    return item_id, items_and_sims[:n]

def TrainAndComputeRecommendation(sc, datafile, 
                                  num_to_recomm_per_user=10, 
                                  num_to_recomm_per_item=10):
    ratings = datafile.map(
        
        # =======================================================================================
        lambda l: Rating(int(l['CustomerID']), int(l['ProductID']), float(l['Score']))).cache()
        # =======================================================================================
        
    numRatings = ratings.count()
    numUsers = ratings.map(lambda r: r[0]).distinct().count()
    numItems = ratings.map(lambda r: r[1]).distinct().count()
    print("Got %d ratings from %d users on %d items." 
        % (numRatings, numUsers, numItems))
    # print (ratings.collect())
    # Matrix Factorization
#   model = train_model_with_validation_test(ratings)
#   model = train_model_with_validation(ratings)
    model = train_model_with_preset_param(ratings)

    # Save the model
#   model.save(sc, "target/tmp/PysparkCollaborativeFiltering")

    # Recommend items for all users
    recomm_items = model.recommendProductsForUsers(numItems).map(
        lambda x: (x[0],[r[1] for r in x[1]]))
    user_items = datafile.map(
        lambda l: (int(l['CustomerID']),int(l['ProductID']))).groupByKey()
    recomm_items_per_user = recomm_items.join(user_items).map(
        lambda x: (x[0],
                   [r for r in x[1][0] if not r in x[1][1]][:num_to_recomm_per_user]))

    # Computer item based similarity
    user_item_pairs = datafile.map(
        lambda l: ((int(l['CustomerID']),int(l['ProductID'])),float(l['Score']))).reduceByKey( # in case of multiple purchases
        lambda x,y:x+y).map(
        lambda l: (l[0][0],(l[0][1],l[1]))).groupByKey()
        #.map(lambda p: sampleInteractions(p[0],p[1],500)) # sampling might or might not be necessary
    pairwise_items = user_item_pairs.filter(
        lambda p: len(p[1]) > 1).flatMap(itemPairs).map(toStruct).reduceByKey(structReducer)
    item_sims = pairwise_items.map(calcSim).map(
        lambda p: keyOnFirstItem(p[0],p[1])).groupByKey().map(
        lambda x : (x[0], list(x[1])))
    
    # Recommend items for each item
    recomm_items_per_item = item_sims.map(
        lambda p: nearestNeighbors(p[0],p[1],num_to_recomm_per_item)).map(
        lambda x: (x[0], [r[0] for r in x[1]]))
    
    return recomm_items_per_user,recomm_items_per_item



# ================================================================================
from pymongo import MongoClient
from pyspark import SparkContext, SparkConf
import pymongo_spark

# Important: activate pymongo_spark.
pymongo_spark.activate()

# =================================================================================

if __name__ == "__main__":
    

    # =============================================================================
    sc = SparkContext(appName="PysparkCollaborativeFiltering")
    sc.setCheckpointDir('checkpoint/')
    datafile = sc.mongoRDD('mongodb://localhost:27017/test_database.transactions')
    mongo_client= MongoClient() 
    db = mongo_client.test_database
    db.recomm_per_user.drop()
    db.recomm_per_item.drop()
    mongo_client.close()
    # =============================================================================
    
    num_to_recomm_per_user = 10
    num_to_recomm_per_item = 10
    
    recomm_per_user,recomm_per_item = TrainAndComputeRecommendation(sc, datafile, 
                                                                    num_to_recomm_per_user, 
                                                                    num_to_recomm_per_item)

    recomm_per_user.saveToMongoDB('mongodb://localhost:27017/test_database.recomm_per_user')
    print("%d recommendations per user complete" % num_to_recomm_per_user)

    recomm_per_item.saveToMongoDB('mongodb://localhost:27017/test_database.recomm_per_item')
    print("%d recommendations per item complete" % num_to_recomm_per_item)
    



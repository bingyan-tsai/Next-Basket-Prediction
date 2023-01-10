## Next-Basket-Prediction
created by Jason Tsai from National Yang Ming Chiao Tung University, Taiwan.
[![Analysis-Process.png](https://i.postimg.cc/FKxbXWP8/Analysis-Process.png)](https://postimg.cc/XZpBw86k)

## How to predict customers' next basket? A fundamental implementation
We construct the model through methods like RFM analysis and Apriori to deal with real-world data provided by a well-known retailer. The basic concept of our model is that customers tend to place an order through their "Shopping Experience." On the other hand, if a product is frequently bought by other customers(i.e., a popular product), it is likely to generate a favorable result to improve the performance of our model if we add them into the final basket. 
Based on the knowledge above, we predict customers' next basket through their order history and popular products during each period by segment after a simple RFM analysis. 
Furthermore, we introduced the Apriori algorithm to the model at the end of the prediction.

## Environment

```
Python 3.9.12
```

## Installation

```
os
time
tqdm
pandas
apyori
pymysql
```





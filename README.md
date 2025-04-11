Names: Alexander Sharpe and Cristobal Benavides

# Part 1: verifyMIS on Local Machine 

|        Graph file       |           MIS file           | Is an MIS? |
| ----------------------- | ---------------------------- | ---------- |
| small_edges.csv         | small_edges_MIS.csv          | Yes        |
| small_edges.csv         | small_edges_non_MIS.csv      | No         |
| line_100_edges.csv      | line_100_MIS_test_1.csv      |  Yes         |
| line_100_edges.csv      | line_100_MIS_test_2.csv      |    No       |
| twitter_10000_edges.csv | twitter_10000_MIS_test_1.csv |     Yes      |
| twitter_10000_edges.csv | twitter_10000_MIS_test_2.csv |    Yes       |

# Part 2: LubyMIS on Local Machine 
| Graph file              | Iterations | Running Time (s) | MIS Size | MIS? |
|------------------------|------------|------------------|----------|------|
| small_edges.csv        | 1          | 0.73             | 2        | yes  |
| line_100_edges.csv     | 3          | 0.86             | 41       | yes  |
| twitter_100_edges.csv  | 2          | 0.72             | 96       | yes  |
| twitter_1000_edges.csv | 3          | 0.93             | 951      | yes  |
| twitter_10000_edges.csv| 3          | 1.58             | 9658     | yes  |


# Part 3: LubyMIS on twitter_original_edges.csv in GCP 

For the 3x4 and 2x2 configurations we used 100 GB disk space and Balanced Persistent Disk type. For the 4x2 configuration we had to use the Standard Disk type due to repeated quota errors. 

| Cores | Iterations | Running Time (s) | Remaining Active Vertices (After Each Iteration) | MIS? |
| ----- | ---------- | ------------ | ------------------------- | ---- |
| 3x4   |     5       | 166.92 | 6666259, 33378, 492, 1, 0 |   Yes   |
| 4x2   |     5       |              | 6865939, 39499, 518, 3, 0   |   Yes   |
| 2x2   |     7       |       1213.07       | 6993185, 41682, 5706, 109, 9, 1, 0   |    Yes  |


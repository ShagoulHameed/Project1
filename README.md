**YouTube Data Harvesting and Warehousing**

YouTube Data Harvesting and Warehousing is a project that aims to provide users with the ability to access and analyze data from various YouTube channels. The project utilizes SQL, MongoDB, and Streamlit to develop a user-friendly application that enables users to retrieve, save, and query YouTube channel and video data efficiently.

**Tools and Libraries Used**
Streamlit
Streamlit library was used to create a user-friendly UI that enables users to interact with the program and carry out data retrieval and analysis operations.

**Python**
Python is the primary programming language employed in this project for the development of the complete application, including data retrieval, processing, analysis, and visualization.

**Google API Client**
The googleapiclient library in Python facilitates communication with different Google APIs, particularly YouTube's Data API v3, allowing the retrieval of essential information such as channel details, video specifics, and comments.

**MongoDB Atlas**
MongoDB Atlas is utilized to store the data obtained from YouTube's Data API v3. It provides a fully managed and scalable database solution for efficient data management.

**PostgreSQL**
PostgreSQL is used for structured data storage and management, offering advanced SQL capabilities for efficient querying and analysis.

YouTube Data Scraping and Ethical Perspective
When engaging in the scraping of YouTube content, it is crucial to approach it ethically and responsibly. Respecting YouTube's terms and conditions, obtaining appropriate authorization, and adhering to data protection regulations are fundamental considerations. The collected data must be handled responsibly, ensuring privacy, confidentiality, and preventing any form of misuse or misrepresentation.

**Required Libraries**
googleapiclient.discovery
streamlit
psycopg2
pymongo
pandas
Features
The YouTube Data Harvesting and Warehousing application offers the following functions:

Retrieval of channel and video data from YouTube using the YouTube API.
Storage of data in a MongoDB database as a data lake.
Migration of data from the data lake to a SQL database for efficient querying and analysis.
Search and retrieval of data from the SQL database using different search options.
Additional Resources
You can view a video demonstration of this project on my LinkedIn profile:
YouTube Data Harvesting and Warehousing - LinkedIn Video

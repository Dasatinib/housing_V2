# housing_V2
Are you looking for a rental at a specific location in the Czech Republic but insufficient current data exists?
Visit www.NajemChytre.cz to see historical rental data!
The website helps renters and landlords alike to see fair standard prices and make better informed decisions.

Rental prices differ based on property characteristics including age, location and furnishing but also surrounding amenities, noise pollution, sunlight and time of the year. In order to make an informed decision, retners and landlords need not only city/district averages but specific street/building data that is directly comparable to their situation.
www.NajemChytre.cz currently offers year-long historical data for the Czech Republic with listing-level granularity for you to make better informed decisions about your accomodation.

Current website version runs on a depriciated code.
This repository contains updated but not yet deployed code.

Improvements include:
- Full listing data retention.
- Overall improved code scalability and modularity.
- Listing image storage

TBD before deployment:
- LLM processing of rental description for price and description
- Updat frontend

TBD long-term
Priority A
- Merge listings according to buildings and flats (eg. same two listings from 2 different sites, two listings that appear for the same flat from a year apart)
- Remove listings that don't have exact address
- Add SR
Priority B
- Easy to access statistics
- Include noise map data
Priority C
- Notifications for newly available listings
- AI-enhanced search for housing

Other ideas
- Make a price/location guessing game


Known bugs
- If a listing has been removed when downloaded it can break the html logic. See "Removed listings" in "bug_examples"

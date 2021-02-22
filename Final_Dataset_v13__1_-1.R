
#install.packages("kernlab")
#install.packages("Information")
#install.packages("woeBinning")
library(tidymodels)
library(lubridate)
library(stringr)
library(xgboost)
library(ranger)
library(vip)
library(corrplot)
library(GGally)
library(doParallel)
library(themis)
library(probably)
library(tidyr)
library(summarytools)
library(rsample)
library(caret) 
library(tidyverse)
library(workflows)
library(tune)
library(kernlab)
library(themis)
library(strex)
library(dplyr)
library(Information)
library(mlbench)
library(woeBinning)
set.seed(2021)
###########Physicians###############
physicians_raw <- readr::read_csv('physicians.csv')

physicians <- physicians_raw %>% 
  select(-Zipcode, -Name_Suffix, -Middle_Name, -First_Name, -Province, -Last_Name)

physicians %>% count(Primary_Specialty, sort = T) %>% filter(n<10)
#Split the Primary Speciality column into three
specialities <- str_split_fixed(
  physicians_raw$Primary_Specialty,'\\|', n=3) 
physicians <- physicians %>% 
  bind_cols(as_tibble(specialities)) %>% 
  select(-Primary_Specialty) %>%
  rename(Primary_Speciality_General = V1, 
         Primary_Speciality_Main = V2, 
         Primary_Speciality_Sub = V3)

physicians <- physicians %>% 
  mutate(Primary_Speciality_General = 
           if_else(Primary_Speciality_General == "", NA_character_, Primary_Speciality_General),
         Primary_Speciality_Main = 
           if_else(Primary_Speciality_Main == "", NA_character_, Primary_Speciality_Main),
         Primary_Speciality_Sub = 
           if_else(Primary_Speciality_Sub == "", NA_character_, Primary_Speciality_Sub))

Num_State_License <- physicians %>% 
  pivot_longer(License_State_1:License_State_5, 
               values_to = 'License_States') %>% 
  select(id, License_States) %>% 
  group_by(id) %>% 
  na.omit() %>% 
  count(id)

physicians <- physicians %>% 
  inner_join(Num_State_License, by='id') %>% 
  rename(Num_State_License = n) %>% 
  relocate(Num_State_License, .after = License_State_5)

physicians <- physicians %>% mutate(
  across(
    .cols=starts_with('License'),
    .fns= ~if_else(is.na(.), 'None', .)))

physicians <- physicians %>% rename(
  Physician_State = State,
  Physician_City = City,
  Physician_Country = Country 
)
#############Payments#############
payments_raw<-read_csv('payments.csv', col_types = cols(Product_Category_1 = col_character() ,Product_Category_2 = col_character(),Product_Category_3 = col_character()))



payments <- payments_raw %>% select(-Product_Name_1 ,-Product_Name_2, -Product_Name_3, -Product_Category_1, -Product_Category_2, -Product_Category_3)


payments <- payments %>% mutate(Date = as.Date(Date, format='%m/%d/%Y'))

payments <- payments %>% mutate(Year = year(Date),
                                Quarter = quarter(Date)) %>% relocate(Quarter, Year, .after=Date)

payments <- payments %>% mutate(
  across(
    .cols=c(City_of_Travel, Country_of_Travel, State_of_Travel), 
    .fns = ~if_else(is.na(.), 'None', .)))

payments <- payments %>% mutate(
  Product_Type_1 = case_when(
    Product_Type_1 %in% c('Biological', 'Drug', 'Drug or Biological') ~ 'Drug/Biological',
    Product_Type_1 %in% c('Device','Device or Medical Supply', 'Medical Supply') ~ 'Device/Medical Supply',
    # TRUE ~ Product_CType_1
  ),
  Product_Type_2 = case_when(
    Product_Type_2 %in% c('Biological', 'Drug', 'Drug or Biological') ~ 'Drug/Biological',
    Product_Type_2 %in% c('Device','Device or Medical Supply', 'Medical Supply') ~ 'Device/Medical Supply',
    TRUE ~ Product_Type_2
  ),
  Product_Type_3 = case_when(
    Product_Type_3 %in% c('Biological', 'Drug', 'Drug or Biological') ~ 'Drug/Biological',
    Product_Type_3 %in% c('Device','Device or Medical Supply', 'Medical Supply') ~ 'Device/Medical Supply',
    TRUE ~ Product_Type_3
  ))

payments <- payments %>% 
  mutate(Form_of_Payment_or_Transfer_of_Value = 
           case_when(
             grepl('Stock', Form_of_Payment_or_Transfer_of_Value) | 
               grepl('Dividend', Form_of_Payment_or_Transfer_of_Value) |
               grepl('Any', Form_of_Payment_or_Transfer_of_Value) ~ 'Ownership Interest',
             TRUE ~ Form_of_Payment_or_Transfer_of_Value
           ))

payments <- payments %>% mutate(
  Related_Product_Indicator = 
    case_when(
      Related_Product_Indicator %in% c('No', 'None') ~ 'No',
      Related_Product_Indicator %in% c('Yes', 'Covered', 'Non-Covered', 'Combination') ~ 'Yes'
    )
)

payments <- payments %>% mutate(
  Third_Party_Recipient = 
    if_else(Third_Party_Recipient %in% c('Entity', 'Individual'), 'Yes', 'No')
)

payments_filtered <- payments %>% filter(Ownership_Indicator == 'No')

#################Companies###########
companies_raw <- readr::read_csv('companies.csv')
companies <- companies_raw %>% select(-Name) %>% rename(Company_Country = Country,
                                                        Company_State = State
)


####################Joined Table###################
df <- payments %>% inner_join(physicians, by=c('Physician_ID'='id'))
df <- df %>% inner_join(companies_raw, by='Company_ID')
df_train <- df %>% filter(set=='train')

df_filtered <- payments_filtered %>% 
  inner_join(physicians, by=c('Physician_ID'='id')) %>%
  inner_join(companies_raw, by='Company_ID')

df_filtered <- df_filtered %>% rename(
  Company_Name = Name,
  Company_State = State,
  Company_Country = Country
)


prop_wider_by_Physician <- function(df,col) {
  x <- enquo(col)
  df %>% group_by(Physician_ID) %>% 
    dplyr::count(!!x) %>% 
    dplyr::summarize(!!x, Prop = n/sum(n)) %>%
    pivot_wider(names_from=!!x, values_from=Prop, 
                names_prefix = paste0(df %>% select(!!x) %>% colnames, 
                                      sep='_'))
}
#Categorical Variables in Transactions data
Third_Party_Recipient <- prop_wider_by_Physician(df_filtered,Third_Party_Recipient)
Related_Product_Indicator <- prop_wider_by_Physician(df_filtered,Related_Product_Indicator)
Form_of_Payment_or_Transfer_of_Value <- prop_wider_by_Physician(df_filtered,Form_of_Payment_or_Transfer_of_Value)
Third_Party_Covered <- prop_wider_by_Physician(df_filtered,Third_Party_Covered)
Charity <- prop_wider_by_Physician(df_filtered,Charity)
Product_Type_1 <- prop_wider_by_Physician(df_filtered, Product_Type_1)
Transactions_per_Year <- df_filtered %>% group_by(Physician_ID) %>% 
  count(Year) %>% 
  summarize(Year, Prop = n/sum(n)) %>%
  pivot_wider(names_from=Year, values_from=Prop, 
              names_prefix = 'Transactions_in_')

Product_Type_Long <- df_filtered %>% pivot_longer(Product_Type_1:Product_Type_3,
                                                  values_to = 'Product_Type')
Product_Type_Long <- prop_wider_by_Physician(Product_Type_Long, Product_Type)

#Payment variables in Transactions data
Amount_Payments <- df_filtered %>% 
  group_by(Physician_ID) %>% 
  summarize(Total_Amount_of_Payment = sum(Total_Amount_of_Payment_USDollars),
            Avg_Amount_of_Payment = mean(Total_Amount_of_Payment_USDollars)) 
Num_Payments_Greater_1 <- df_filtered %>% group_by(Physician_ID) %>%
  filter(Number_of_Payments>1) %>%
  summarize(Total_Num_Payments_Greater_1 = sum(Number_of_Payments),
            Count_Num_Payments_Greater_1 = n())

Total_Amount_of_Payment_by_Year <- df_filtered %>%
  group_by(Physician_ID, Year) %>%
  summarize( 
    Total_Amount_of_Payment_by_Year = sum(Total_Amount_of_Payment_USDollars)) %>%
  pivot_wider(names_from=Year, values_from=Total_Amount_of_Payment_by_Year, 
              names_prefix = 'Total_Amount_of_Payment_in_')

#Variables about Travels
Number_of_Local_Companies <- df_filtered %>% mutate(Company_State=if_else(is.na(Company_State), 'None', Company_State)) %>%
  mutate(P_State_C_State = if_else(Physician_State==Company_State, 1, 0)) %>%
  group_by(Physician_ID) %>%
  summarize(Number_of_Local_Companies = sum(P_State_C_State))

Country_of_Travel_Abroad_Total <- df_filtered %>% group_by(Physician_ID) %>% 
  filter(!Country_of_Travel %in% c('None','United States')) %>% 
  count(Country_of_Travel) %>%
  summarise(Country_of_Travel_Abroad_Total = sum(n))
State_of_Travel_Total <- df_filtered %>% group_by(Physician_ID) %>%
  filter(State_of_Travel!='None') %>%
  count(State_of_Travel) %>%
  summarize(State_of_Travel_Total =sum(n))
Travel_to_Company_Abroad_Country <- df_filtered %>% group_by(Physician_ID) %>%
  filter(Country_of_Travel%in%c('None', 'United States')) %>%
  count(TravelCountry_CompanyCountry=Country_of_Travel==Company_Country) %>% 
  summarise(TravelCountry_CompanyCountry,Travel_to_Company_Country = n/sum(n)) %>%
  filter(TravelCountry_CompanyCountry==TRUE) %>% select(Travel_to_Company_Country)
Travel_to_Company_State <- df_filtered %>% group_by(Physician_ID) %>%
  filter(State_of_Travel!='None') %>%
  count(TravelState_CompanyState=State_of_Travel==Company_State) %>% 
  summarise(TravelState_CompanyState,Travel_to_Company_State = n/sum(n)) %>%
  filter(TravelState_CompanyState==TRUE) %>% select(Travel_to_Company_State)

physicians <- physicians %>% rename(Physician_ID = id)

Ownership_Interest <- df %>% group_by(Physician_ID) %>%
  mutate(Ownership_Indicator = 
           if_else(Ownership_Indicator=='No', 0, 1)) %>%
  summarize(Num_Ownership = sum(Ownership_Indicator)) %>%
  transmute(Physician_ID,Ownership_Interest = if_else(Num_Ownership > 0, 1, 0))

physicians_col_ata <-  Number_of_Local_Companies%>% 
  inner_join(Third_Party_Covered, by='Physician_ID') %>%
  inner_join(Third_Party_Recipient, by='Physician_ID') %>%
  inner_join(Form_of_Payment_or_Transfer_of_Value, by='Physician_ID') %>%
  inner_join(Charity, by='Physician_ID') %>%
  inner_join(Product_Type_Long, by='Physician_ID') %>% 
  inner_join(Amount_Payments, by='Physician_ID') %>%
  full_join(Num_Payments_Greater_1, by='Physician_ID')
physicians_col_ata <- physicians_col_ata %>% 
  full_join(Country_of_Travel_Abroad_Total, by='Physician_ID') %>%
  full_join(State_of_Travel_Total, by='Physician_ID') %>%
  full_join(Travel_to_Company_Abroad_Country, by='Physician_ID') %>%
  full_join(Travel_to_Company_State, by='Physician_ID') %>%
  inner_join(Ownership_Interest, by='Physician_ID') %>%
  full_join(Transactions_per_Year, by='Physician_ID')





##########################Jonas##################
payments_df<-read_csv('payments.csv', col_types = cols(Contextual_Information = col_character(), Product_Category_1 = col_character() ,Product_Category_2 = col_character(),Product_Category_3 = col_character()))

payments_df <- filter(payments_df, Ownership_Indicator == "No")

# avg number of transations per company
a <-payments_df %>% 
  group_by(Physician_ID) %>%
  summarise(count_distinct_companies = n_distinct(Company_ID),
            amount_transactions = n()) %>%
  summarise(Physician_ID = Physician_ID,
            avg_transactions_per_company = amount_transactions/count_distinct_companies)

# of companies transactions came from**********************************************************************************************
b <- payments_df %>% 
  group_by(Physician_ID) %>%
  summarise(count_distinct_companies = n_distinct(Company_ID))
#**********************************************************************************************************************************

#proportion of transactions for each nature of payment type************************************************************************
payments_df <- payments_df %>% mutate(nature_of_payment_other = case_when((grepl("Charitable Contribution", Nature_of_Payment_or_Transfer_of_Value) | 
                                                                             grepl("Compensation for serving as faculty or as a speaker for an accredited or certified continuing education program", Nature_of_Payment_or_Transfer_of_Value) |
                                                                             grepl("Compensation for serving as faculty or as a speaker for a non-accredited and noncertified continuing education program", Nature_of_Payment_or_Transfer_of_Value) |
                                                                             grepl("Grant", Nature_of_Payment_or_Transfer_of_Value) |
                                                                             grepl("Current or prospective ownership or investment interest", Nature_of_Payment_or_Transfer_of_Value) |
                                                                             grepl("Royalty or License", Nature_of_Payment_or_Transfer_of_Value)) ~ 1,
                                                                          TRUE ~ 0),
                                      nature_of_payment_science_worker = case_when(grepl("Compensation for services other than consulting, including serving as faculty or as a speaker at a venue other than a continuing education program", Nature_of_Payment_or_Transfer_of_Value) ~ 1,
                                                                                   TRUE ~ 0),
                                      nature_of_payment_consulting = case_when(grepl("Consulting Fee", Nature_of_Payment_or_Transfer_of_Value) ~ 1,
                                                                               TRUE ~ 0),
                                      nature_of_payment_education = case_when(grepl("Education", Nature_of_Payment_or_Transfer_of_Value) ~ 1,
                                                                              TRUE ~ 0),
                                      nature_of_payment_entertainment = case_when(grepl("Entertainment", Nature_of_Payment_or_Transfer_of_Value) ~ 1,
                                                                                  TRUE ~ 0),
                                      nature_of_payment_food = case_when(grepl("Food and Beverage", Nature_of_Payment_or_Transfer_of_Value) ~ 1,
                                                                         TRUE ~ 0),
                                      nature_of_payment_gift = case_when(grepl("Gift", Nature_of_Payment_or_Transfer_of_Value) ~ 1,
                                                                         TRUE ~ 0),
                                      nature_of_payment_honoria = case_when(grepl("Honoraria", Nature_of_Payment_or_Transfer_of_Value) ~ 1,
                                                                            TRUE ~ 0),
                                      nature_of_payment_travel = case_when(grepl("Travel and Lodging", Nature_of_Payment_or_Transfer_of_Value) ~ 1,
                                                                           TRUE ~ 0)
)

c <- payments_df %>% 
  group_by(Physician_ID) %>%
  summarise(total_transactions = n(),
            Nature_of_Payment_Other = sum(nature_of_payment_other)/total_transactions,
            Nature_of_Payment_Science_Worker = sum(nature_of_payment_science_worker)/total_transactions,
            Nature_of_Payment_Consulting = sum(nature_of_payment_consulting)/total_transactions,
            Nature_of_Payment_Education = sum(nature_of_payment_education)/total_transactions,
            Nature_of_Payment_Entertainment = sum(nature_of_payment_entertainment)/total_transactions,
            Nature_of_Payment_Food = sum(nature_of_payment_food)/total_transactions,
            Nature_of_Payment_Gift = sum(nature_of_payment_gift)/total_transactions,
            Nature_of_Payment_Honoria = sum(nature_of_payment_honoria)/total_transactions,
            Nature_of_Payment_Travel = sum(nature_of_payment_travel)/total_transactions
  )

#**********************************************************************************************************************************

# of travels to distinct...********************************************************************************************************
#cities
d <- payments_df %>% 
  group_by(Physician_ID) %>%
  summarise(Distinct_Travel_Cities = n_distinct(tolower(City_of_Travel)))

#states
e <- payments_df %>% 
  group_by(Physician_ID) %>%
  summarise(Distinct_Travel_States = n_distinct(State_of_Travel))

#countries
f <- payments_df %>% 
  group_by(Physician_ID) %>%
  summarise(Distinct_Travel_Countries = n_distinct(Country_of_Travel))
#**********************************************************************************************************************************

#avg days between date and previous date grouped by doc id ************************************************************************
payments_df <- payments_df %>% mutate(Date = as.Date(Date, format='%m/%d/%Y'))


payments_df <- payments_df %>%
  arrange(Physician_ID,Date)

payments_df$DateDiff <- ave(as.numeric(payments_df$Date), payments_df$Physician_ID, FUN=function(x) c(abs(diff(x)),0))

g <- payments_df %>% 
  group_by(Physician_ID) %>%
  summarise(avg_days_between_payments = mean(DateDiff))
#**********************************************************************************************************************************

#avg days between date and previous date from best paying company grouped by doc id ************************************************************************
physicians_best_paying_company <- payments_df %>%
  group_by(Physician_ID, Company_ID) %>%
  summarise(amount_payed = sum(Total_Amount_of_Payment_USDollars)) %>%
  group_by(Physician_ID) %>%
  summarise(Company_ID = Company_ID[amount_payed==max(amount_payed)]) %>%
  filter(!((Physician_ID == 2987) & (Company_ID == 1534))) #single duplicate elimination

payments_best_paying_company <- physicians_best_paying_company %>%
  left_join(payments_df, by= c('Physician_ID' = 'Physician_ID', 'Company_ID' = 'Company_ID'))

payments_best_paying_company <- payments_best_paying_company %>%
  arrange(Physician_ID, Date)

payments_best_paying_company$DateDiff <- ave(as.numeric(payments_best_paying_company$Date), payments_best_paying_company$Physician_ID, FUN=function(x) c(abs(diff(x)),0))

j <- payments_best_paying_company %>% 
  group_by(Physician_ID) %>%
  summarise(avg_days_between_payments_top_company = mean(DateDiff))
#**********************************************************************************************************************************


companies_df <- read_csv("companies.csv")
# of transactions by company legal form ************************************************************************
companies_df <- companies_df %>% mutate(company_legal_form_Corp = case_when((grepl("Inc", Name) | grepl("Corp", Name)) ~ 1,
                                                                            TRUE ~ 0),
                                        company_legal_form_LC = case_when((grepl("LC", Name) | grepl("Ltd", Name) | grepl("CO.", Name))  ~ 1,
                                                                          TRUE ~ 0),
                                        company_legal_form_Partnerships = case_when(grepl("LP", Name) ~ 1,
                                                                                    TRUE ~ 0))

companies_n_payments <- payments_df %>% 
  full_join(companies_df, by='Company_ID')

h <- companies_n_payments %>% 
  group_by(Physician_ID) %>%
  summarise(sum_company_legal_form_Corp = sum(company_legal_form_Corp),
            sum_company_legal_form_LC = sum(company_legal_form_LC),
            sum_company_legal_form_Partnerships = sum(company_legal_form_Partnerships))
#**********************************************************************************************************************************
# legal form top paying company ********************************************************************************************
physicians_best_paying_company <- payments_df %>%
  group_by(Physician_ID, Company_ID) %>%
  summarise(amount_payed = sum(Total_Amount_of_Payment_USDollars)) %>%
  group_by(Physician_ID) %>%
  summarise(Company_ID = Company_ID[amount_payed==max(amount_payed)]) %>%
  filter(!((Physician_ID == 2987) & (Company_ID == 1534))) #single duplicate elimination

physicians_n_best_paying_company <- physicians_best_paying_company %>%
  right_join(companies_df, by= c('Company_ID' = 'Company_ID'))

k <- physicians_n_best_paying_company %>% 
  mutate(top_company_legal_form = case_when(company_legal_form_Corp == 1 ~ "Corporation",
                                            company_legal_form_LC == 1 ~ "Limited_Liabillity",
                                            company_legal_form_Partnerships == 1 ~ "Partnership",
                                            TRUE ~ "None")) %>%
  select(Physician_ID, top_company_legal_form) %>%
  na.omit()
  
#**********************************************************************************************************************************

# of distinct companies with Ownership interest ************************************************************************
payments_w_ownership <- read_csv('payments.csv', col_types = cols(Contextual_Information = col_character(), Product_Category_1 = col_character() ,Product_Category_2 = col_character(),Product_Category_3 = col_character()))

i <- payments_w_ownership %>% 
  mutate(ownership_company = case_when(grepl("Yes", Ownership_Indicator) ~ 1,
                              TRUE ~ 0)) %>%
  group_by(Company_ID) %>%
  summarise(sum_ownership_company = sum(ownership_company)) %>%
  mutate(ownership_company = case_when(sum_ownership_company > 0 ~ 1,
                                     TRUE ~ 0)) %>%
  select(Company_ID, ownership_company) %>%
  full_join(payments_df, by='Company_ID') %>%
  filter(ownership_company != 0) %>%
  group_by(Physician_ID) %>%
  summarize(distinct_ownership_companies = n_distinct(Company_ID)) %>%
  na.omit()
#**********************************************************************************************************************************

# of outlier transaction by company ************************************************************************
company_transaction_quantile <- payments_df %>%
  group_by(Company_ID) %>%
  summarise(amount_last_quantile = quantile(Total_Amount_of_Payment_USDollars, c(0.999)))

l <- company_transaction_quantile %>%
  right_join(payments_df, by='Company_ID') %>%
  mutate(outlier_transaction = case_when(Total_Amount_of_Payment_USDollars > amount_last_quantile ~ 1,
                                         TRUE ~ 0)) %>%
  group_by(Physician_ID) %>%
  summarise(outlier_transactions = sum(outlier_transaction))
#**********************************************************************************************************************************

payments_jonas <- a %>% 
  full_join(b, by='Physician_ID') %>%
  full_join(c, by='Physician_ID') %>%
  full_join(d, by='Physician_ID') %>%
  full_join(e, by='Physician_ID') %>%
  full_join(f, by='Physician_ID') %>%
  full_join(g, by='Physician_ID') %>%
  full_join(h, by='Physician_ID') %>%
  full_join(i, by='Physician_ID') %>%
  full_join(j, by='Physician_ID') %>%
  full_join(k, by='Physician_ID') %>% 
  full_join(l, by='Physician_ID')

payments_jonas <- payments_jonas %>%
  mutate(distinct_ownership_companies = case_when(is.na(distinct_ownership_companies) ~ 0,
                                                  TRUE ~ as.numeric(distinct_ownership_companies))) %>%
  na.omit()


physicians_col_ata_jonas <- physicians_col_ata %>%
  inner_join(payments_jonas, by='Physician_ID')


###################Ena#######################

options(dplyr.width = Inf)
theme_set(theme_minimal()) 


############################Payments read-in and cleanup
#----

payments <- read_csv(
  "payments.csv",
  col_types = cols(
    Product_Category_1 = col_character(),
    Product_Category_2 = col_character(),
    Product_Category_3 = col_character()
  )
)

payments <- payments %>% mutate(
  Product_Category_1 = toupper (Product_Category_1),
  Product_Category_2 = toupper (Product_Category_2),
  Product_Category_3 = toupper (Product_Category_3)
)


physicians <- read_csv(
  "physicians.csv",
  col_types = cols(
    Province = col_character())
)

physicians <- physicians %>% select(-Province, -Name_Suffix, -First_Name, 
                                    -Middle_Name, -Last_Name, -Country)

physicians[physicians$id==4689,]$State <- "FL"

specialities <- str_split_fixed(
  physicians$Primary_Specialty, '\\|', n=3) 

physicians <- physicians %>% 
  bind_cols(as_tibble(specialities)) %>% 
  select(-Primary_Specialty) %>%
  rename(Primary_Speciality_General = V1, 
         Primary_Speciality_Main = V2, 
         Primary_Speciality_Sub = V3)


physicians <- physicians %>% 
  mutate(Primary_Speciality_General = 
           if_else(Primary_Speciality_General == "", NA_character_, Primary_Speciality_General),
         Primary_Speciality_Main = 
           if_else(Primary_Speciality_Main == "", NA_character_, Primary_Speciality_Main),
         Primary_Speciality_Sub = 
           if_else(Primary_Speciality_Sub == "", NA_character_, Primary_Speciality_Sub))

Num_State_License <- physicians %>% 
  pivot_longer(License_State_1:License_State_5, 
               values_to = 'License_States') %>% 
  select(id, License_States) %>% 
  group_by(id) %>% 
  na.omit() %>% 
  count(id)

physicians <- physicians %>% 
  inner_join(Num_State_License, by='id') %>% 
  rename(Num_State_License = n) %>% 
  relocate(Num_State_License, .after = License_State_5)

physicians <- physicians %>% mutate(
  across(
    .cols=starts_with('License'),
    .fns= ~if_else(is.na(.), 'None', .)))


physicians <- physicians %>% mutate(
  Zipcode = str_before_nth(Zipcode, pattern = "|", n=3))

ownership <- payments %>% filter(Ownership_Indicator=="Yes") %>% select(Physician_ID, Ownership_Indicator)
no_ownership <- payments %>% filter(Ownership_Indicator=="No") %>% select(Physician_ID, Ownership_Indicator)

ownership <- ownership %>% group_by(Physician_ID) %>% pivot_wider()
no_ownership <- no_ownership %>% group_by(Physician_ID) %>% pivot_wider()
no_ownership <- anti_join(no_ownership, ownership, by = "Physician_ID")

Label <- rbind(ownership, no_ownership)
Label <- Label %>% rename(Owner = Ownership_Indicator)

physicians <- physicians %>% 
  left_join(Label, by = c("id" = "Physician_ID")) %>% 
  relocate(Owner, .after = set)

physicians <- physicians %>% mutate(
  Owner = if_else(Owner == "Yes", 1L, 0L))

rm(Label)
rm(ownership)
rm(no_ownership)


###########################Preparing the dataset

#Take out all transactions where ownership indicator == Yes
payments <- payments %>% filter(Ownership_Indicator=="No")


#Most common product category 1 exclusing NAs
#-----
Most_Common_Prod_Cat <- payments %>% 
  select(Physician_ID, Product_Category_1, Total_Amount_of_Payment_USDollars) 

Most_Common_Prod_Cat <- Most_Common_Prod_Cat %>% mutate(
  Product_Category_1 = if_else(Product_Category_1 == "ANESTHESIOLOGY (PAIN MANAGEMENT)", "ANESTHESIOLOGY", Product_Category_1),
  Product_Category_1 = if_else(Product_Category_1 == "CARDIAC DIAGNOSTICS", "CARDIAC DIAGNOSTICS AND MONITORING", Product_Category_1),
  Product_Category_1 = if_else(Product_Category_1 == "CARDIAC RESYNCHRONIZATION THERAPY (CRT)", "CARDIAC RESYNCHRONIZATION THERAPY - CRT", Product_Category_1),
  Product_Category_1 = if_else(Product_Category_1 == "CARDIOLOGY AND VASCULAR DISEASES", "CARDIOLOGY/VASCULAR DISEASES", Product_Category_1),
  Product_Category_1 = if_else(Product_Category_1 == "CARDIOVASCULAR & METABOLISM", "CARDIOVASCULAR AND METABOLISM", Product_Category_1),
  Product_Category_1 = if_else(Product_Category_1 == "CENTRAL NERVOUS SYSTEM (CNS)", "CENTRAL NERVOUS SYSTEM", Product_Category_1),
  Product_Category_1 = if_else(Product_Category_1 == "CNS", "CENTRAL NERVOUS SYSTEM", Product_Category_1),
  Product_Category_1 = if_else(Product_Category_1 == "DEVICE", "DEVICES", Product_Category_1),
  Product_Category_1 = if_else(Product_Category_1 == "KNEES AND  HIPS", "KNEES, HIPS", Product_Category_1),
  Product_Category_1 = if_else(Product_Category_1 == "KNEE & HIP", "KNEES, HIPS", Product_Category_1),
  Product_Category_1 = if_else(Product_Category_1 == "KNEE", "KNEES", Product_Category_1),
  Product_Category_1 = if_else(Product_Category_1 == "OBSTETRICSGYNECOLOGY", "OBSTETRICS/GYNECOLOGY", Product_Category_1),
  Product_Category_1 = if_else(Product_Category_1 == "OBSTETRICSGYNECOLOGY WOMEN'S HEALTH", "OBSTETRICS/GYNECOLOGY", Product_Category_1),
  Product_Category_1 = if_else(Product_Category_1 == "OBGYN", "OBSTETRICS/GYNECOLOGY", Product_Category_1),
  Product_Category_1 = if_else(Product_Category_1 == "OPTHAMOLOGY", "OPHTHALMOLOGY", Product_Category_1),
  Product_Category_1 = if_else(Product_Category_1 == "ORTHOPAEDIC", "ORTHOPEDIC", Product_Category_1),
  Product_Category_1 = if_else(Product_Category_1 == "ORTHOPEDIC SURGERY", "ORTHOPEDICS/ORTHOPEDIC SURGERY", Product_Category_1),
  Product_Category_1 = if_else(Product_Category_1 == "OTOLARYNGOLOGY (EAR, NOSE, THROAT)", "OTOLARYNGOLOGY", Product_Category_1),
  Product_Category_1 = if_else(Product_Category_1 == "WOMENS HEALTH", "WOMEN'S HEALTH", Product_Category_1)
)


Most_Common_Prod_Cat <- Most_Common_Prod_Cat %>% 
  group_by(Physician_ID) %>% 
  mutate(most_common = if_else(is.na(Product_Category_1), 0, 1)) %>% 
  group_by(Physician_ID, Product_Category_1) %>% 
  summarise(num = sum(most_common), amount = sum(Total_Amount_of_Payment_USDollars)) %>% 
  ungroup() 

Most_Common_Prod_Cat <- Most_Common_Prod_Cat %>% group_by (Physician_ID) %>% slice_max(num) %>% slice_max(amount)

Most_Common_Prod_Cat <- Most_Common_Prod_Cat %>% rename(
  Most_Common_Prod_Cat_1 = Product_Category_1) %>% select(-num, -amount)

physicians <- physicians %>% 
  left_join(Most_Common_Prod_Cat, by = c("id" = "Physician_ID")) 

physicians <- physicians %>% mutate(Most_Common_Prod_Cat_1 = if_else(is.na(Most_Common_Prod_Cat_1), "NONE", Most_Common_Prod_Cat_1))
physicians <- physicians %>% mutate(Most_Common_Prod_Cat_1 = as.factor(Most_Common_Prod_Cat_1))

train <- physicians %>% filter(set=='train') 
# df <- train %>% select(Owner, Most_Common_Prod_Cat_1)
# df <- df %>% mutate(
#   Owner = if_else(Owner == 1, "Yes", "No"))
# binning <- woebin (df, y = 'Owner', x = 'Most_Common_Prod_Cat_1', positive = "Yes")


IV <- create_infotables(data = train,
                        y = "Owner")

IV_Value <- data.frame(IV$Summary)
IV_Value

Most_Common_Prod_Cat_WOE <- print(IV$Tables$Most_Common_Prod_Cat_1, row.names = F)
Most_Common_Prod_Cat_WOE <- Most_Common_Prod_Cat_WOE %>% select (-N, -IV)  %>% 
                                                         mutate (Percent = round(Percent * 100, 2))


others <- Most_Common_Prod_Cat_WOE %>% filter(Percent <  5)
binned <- Most_Common_Prod_Cat_WOE %>% filter(Percent >= 5)
binned <- binned %>% mutate(Most_Common_Prod_Cat_WOE = WOE)

others <- others %>% mutate(
  Most_Common_Prod_Cat_WOE = case_when(
    WOE  >= -2.60  & WOE  <= -1.80  ~ "1",
    WOE  >= -1.20  & WOE  <= -0.59  ~ "2",
    WOE  >= -0.50  & WOE  <= -0.001 ~ "3",
    WOE  ==  0                      ~ "4",
    WOE  >=  0.01  & WOE  <=  0.19  ~ "5",
    WOE  >=  0.20  & WOE  <=  1.15  ~ "6",
    WOE  >=  1.23  & WOE  <=  5.00  ~ "7"
  )
)

a <- others %>% group_by(Most_Common_Prod_Cat_WOE) %>% summarise(Final = mean(WOE))
others <- left_join(others, a, by = "Most_Common_Prod_Cat_WOE") %>%  select(-Most_Common_Prod_Cat_WOE)
others <- others %>% mutate(Most_Common_Prod_Cat_WOE = Final) %>% select(-Final)

Most_Common_Prod_Cat_Comb_WOE <- rbind(binned, others)
Most_Common_Prod_Cat_Comb_WOE <- Most_Common_Prod_Cat_Comb_WOE %>% select(-Percent, -WOE)

physicians <- physicians %>% 
  left_join(Most_Common_Prod_Cat_Comb_WOE, by = c("Most_Common_Prod_Cat_1")) %>% 
  relocate(Most_Common_Prod_Cat_WOE, .after = Most_Common_Prod_Cat_1) %>% 
  select (-Most_Common_Prod_Cat_1)

physicians <- physicians %>% mutate(
  Most_Common_Prod_Cat_WOE = if_else(is.na(Most_Common_Prod_Cat_WOE), 0, Most_Common_Prod_Cat_WOE))
#-----


#Zip target encoding
train <- physicians%>% filter(set=='train')  
test <- physicians %>% filter(set=='test')

IV <- create_infotables(data = train,
                        y = "Owner")

IV_Value <- data.frame(IV$Summary)

zip <- print(IV$Tables$Zipcode, row.names = F)
zip <- zip %>% select (-IV, -N) %>% mutate (Percent = round(Percent * 100, 2))

zip <- zip %>% mutate(
  ZIP_WOE = case_when(
    WOE  >= -1.62  & WOE  <= -0.60  ~ "1",
    WOE  >= -0.54  & WOE  <= -0.32  ~ "2",
    WOE  >= -0.30  & WOE  <= -0.20  ~ "3",
    WOE  >= -0.18  & WOE  <= -0.06  ~ "4",
    WOE  >= -0.052 & WOE  <=  0.05  ~ "5",
    WOE  >=  0.10  & WOE  <=  0.201 ~ "6",
    WOE  >=  0.21  & WOE  <=  0.29  ~ "7",
    WOE  >=  0.30  & WOE  <=  0.64  ~ "8",
    WOE  >=  0.65  & WOE  <=  2.50  ~ "9"
  )
)

a <- zip %>% group_by(ZIP_WOE) %>% summarise(Final = mean(WOE))
zip <- left_join(zip, a, by = "ZIP_WOE") %>%  select(-ZIP_WOE)
zip <- zip %>% mutate(ZIP_WOE = Final) %>% select(-Final, -Percent, -WOE)

physicians <- physicians %>% 
  left_join(zip, by = "Zipcode") %>% 
  relocate(ZIP_WOE, .after = Zipcode)
#----- 

#Doctor Licenses
#-----


# Doctor licenses
# Supervised ratio: https://www.kdnuggets.com/2016/08/include-high-cardinality-attributes-predictive-model.html
# Weight of evidence: https://pkghosh.wordpress.com/2017/10/09/combating-high-cardinality-features-in-supervised-machine-learning/
# Weight of evidence works better for class imbalanced data
# https://www.listendata.com/2015/03/weight-of-evidence-woe-and-information.html



train <- physicians %>% filter(set=='train')  

IV_noValid <- create_infotables(data = train,y = "Owner")

IV_noValid_Value <- data.frame(IV_noValid$Summary)


#From these results, Primary_Speciality_Sub, _Main, Zipcode, number of licenses  have the most information value

license_1 <- print(IV_noValid$Tables$License_State_1, row.names = F)
license_1 <- license_1 %>% select (License_State_1, WOE) %>% rename (LS1_WOE = WOE)

license_2 <- print(IV_noValid$Tables$License_State_2, row.names = F)
license_2 <- license_2 %>% select (License_State_2, WOE) %>% rename (LS2_WOE = WOE)

license_3 <- print(IV_noValid$Tables$License_State_3, row.names = F)
license_3 <- license_3 %>% select (License_State_3, WOE)  %>% rename (LS3_WOE = WOE)

license_4 <- print(IV_noValid$Tables$License_State_4, row.names = F)
license_4 <- license_4 %>% select (License_State_4, WOE) %>% rename (LS4_WOE = WOE)

license_5 <- print(IV_noValid$Tables$License_State_5, row.names = F)
license_5 <- license_5 %>% select (License_State_5, WOE)  %>% rename (LS5_WOE = WOE)

# physicians <- physicians %>% 
#   left_join(license_1, by = c("License_State_1")) %>% 
#   left_join(license_2, by = c("License_State_2")) %>% 
#   left_join(license_3, by = c("License_State_3")) %>% 
#   left_join(license_4, by = c("License_State_4")) %>% 
#   left_join(license_5, by = c("License_State_5")) %>% 
#   relocate(LS1_WOE, .after = License_State_1) %>% 
#   relocate(LS2_WOE, .after = License_State_2) %>% 
#   relocate(LS3_WOE, .after = License_State_3) %>% 
#   relocate(LS4_WOE, .after = License_State_4) %>% 
#   relocate(LS5_WOE, .after = License_State_5) 




#COMBINED WOE FOR THE LICENSE STATES
#License State 1
a <- table(train$License_State_1) %>% as.data.frame()
a <- a %>% mutate(perc = round((Freq/sum(Freq)) * 100, 2))

a <- a %>% left_join(license_1, by = c("Var1" = "License_State_1" ))
a <- a %>% mutate(LS1 = if_else(perc >= 4.5, Var1, "Other"))

others <- a %>% filter(LS1 == "Other")
others <- others [order(others$LS1_WOE),]
binned <- anti_join(a, others, by = "Var1")
binned <- binned %>% mutate(LS1 = LS1_WOE)

others <- others %>% mutate(
  LS1 = case_when(
    LS1_WOE  >= -1.30  & LS1_WOE  <= -1.00 ~ "1",
    LS1_WOE  >= -0.80  & LS1_WOE  <= -0.43 ~ "2",
    LS1_WOE  >= -0.38  & LS1_WOE  <= -0.30 ~ "3",
    LS1_WOE  >= -0.295 & LS1_WOE  <= -0.19 ~ "4",
    LS1_WOE  >= -0.12  & LS1_WOE  <= -0.03 ~ "5",
    LS1_WOE  >= -0.022 & LS1_WOE  <=  0.05 ~ "6",
    LS1_WOE  >=  0.07  & LS1_WOE  <=  0.11 ~ "7",
    LS1_WOE  >=  0.12  & LS1_WOE  <=  0.19 ~ "8",
    LS1_WOE  >=  0.22  & LS1_WOE  <=  0.38 ~ "9",
    LS1_WOE  >=  0.40  & LS1_WOE  <=  0.48 ~ "10",
    LS1_WOE  >=  0.50  & LS1_WOE  <=  3.00 ~ "11"
  )
)

a <- others %>% group_by(LS1) %>% summarise(Final = mean(LS1_WOE))
others <- left_join(others, a, by = "LS1")
others <- others %>% mutate( LS1 = Final) %>% select(-Final)

LS1 <- rbind(binned, others)
LS1 <- LS1 %>% select(Var1, LS1) %>% rename(LS1_Combined_WOE = LS1)

physicians <- physicians %>% 
  left_join(LS1, by = c("License_State_1" = "Var1")) %>% 
  relocate(LS1_Combined_WOE, .after = License_State_1)



#License State 2
a <- table(train$License_State_2) %>% as.data.frame()
a <- a %>% mutate(perc = round((Freq/sum(Freq)) * 100, 2))
a <- a %>% left_join(license_2, by = c("Var1" = "License_State_2" ))
a <- a %>% mutate(LS2 = if_else(perc >= 4.5, Var1, "Other"))

others <- a %>% filter(LS2 == "Other")
others <- others [order(others$LS2_WOE),]
binned <- anti_join(a, others, by = "Var1")
binned <- binned %>% mutate(LS2 = LS2_WOE)

others <- others %>% mutate(
  LS2 = case_when(
    LS2_WOE  >= -0.78  & LS2_WOE  <= -0.18 ~ "1",
    LS2_WOE  >= -0.14  & LS2_WOE  <= -0.01 ~ "2",
    LS2_WOE  >=  0.00  & LS2_WOE  <=  0.08 ~ "3",
    LS2_WOE  >=  0.15  & LS2_WOE  <=  0.30 ~ "4",
    LS2_WOE  >=  0.43  & LS2_WOE  <=  0.52 ~ "5",
    LS2_WOE  >=  0.54  & LS2_WOE  <=  0.60 ~ "6",
    LS2_WOE  >=  0.61  & LS2_WOE  <=  0.82 ~ "7",
    LS2_WOE  >=  0.85  & LS2_WOE  <=  2.40 ~ "8"
  )
)

a <- others %>% group_by(LS2) %>% summarise(Final = mean(LS2_WOE))
others <- left_join(others, a, by = "LS2")

others <- others %>% mutate(LS2 = Final) %>% select(-Final)

LS2 <- rbind(binned, others)
LS2 <- LS2 %>% select(Var1, LS2) %>% rename(LS2_Combined_WOE = LS2)

physicians <- physicians %>% 
  left_join(LS2, by = c("License_State_2" = "Var1")) %>% 
  relocate(LS2_Combined_WOE, .after = License_State_2)


#License State 3
a <- table(train$License_State_3) %>% as.data.frame()
a <- a %>% mutate(perc = round((Freq/sum(Freq)) * 100, 2))
a <- a %>% left_join(license_3, by = c("Var1" = "License_State_3" ))
a <- a %>% mutate(LS3 = if_else(perc >= 4.5, Var1, "Other"))

others <- a %>% filter(LS3 == "Other")
others <- others [order(others$LS3_WOE),]
binned <- anti_join(a, others, by = "Var1")
binned <- binned %>% mutate(LS3 = LS3_WOE)

others <- others %>% mutate(
  LS3 = case_when(
    LS3_WOE  >= -0.25  & LS3_WOE  <=  0.001 ~ "1",
    LS3_WOE  >=  0.14  & LS3_WOE  <=  0.77 ~ "2",
    LS3_WOE  >=  0.80  & LS3_WOE  <=  1.04 ~ "3",
    LS3_WOE  >=  1.09  & LS3_WOE  <=  3.45 ~ "4"
  )
)

a <- others %>% group_by(LS3) %>% summarise(Final = mean(LS3_WOE))
others <- left_join(others, a, by = "LS3")

others <- others %>% mutate(LS3 = Final) %>% select(-Final)

LS3 <- rbind(binned, others)
LS3 <- LS3 %>% select(Var1, LS3) %>% rename(LS3_Combined_WOE = LS3)

physicians <- physicians %>% 
  left_join(LS3, by = c("License_State_3" = "Var1")) %>% 
  relocate(LS3_Combined_WOE, .after = License_State_3)


#License State 4
a <- table(train$License_State_4) %>% as.data.frame()
a <- a %>% mutate(perc = round((Freq/sum(Freq)) * 100, 2))
a <- a %>% left_join(license_4, by = c("Var1" = "License_State_4" ))
a <- a %>% mutate(LS4 = if_else(perc >= 4.5, Var1, "Other"))

others <- a %>% filter(LS4 == "Other")
others <- others [order(others$LS4_WOE),]
binned <- anti_join(a, others, by = "Var1")
binned <- binned %>% mutate(LS4 = LS4_WOE)

others <- others %>% mutate(
  LS4 = case_when(
    LS4_WOE  ==  0.00   ~ "1",
    LS4_WOE  >=  0.01  & LS4_WOE  <=  1.4 ~ "2",
    LS4_WOE  >=  1.44  & LS4_WOE  <=  3.45 ~ "3"
  )
)

a <- others %>% group_by(LS4) %>% summarise(Final = mean(LS4_WOE))
others <- left_join(others, a, by = "LS4")
others <- others %>% mutate(LS4 = Final) %>% select(-Final)

LS4 <- rbind(binned, others)
LS4 <- LS4 %>% select(Var1, LS4) %>% rename(LS4_Combined_WOE = LS4)

physicians <- physicians %>% 
  left_join(LS4, by = c("License_State_4" = "Var1")) %>% 
  relocate(LS4_Combined_WOE, .after = License_State_4)


#License State 5
a <- table(train$License_State_5) %>% as.data.frame()
a <- a %>% mutate(perc = round((Freq/sum(Freq)) * 100, 2))
a <- a %>% left_join(license_5, by = c("Var1" = "License_State_5" ))
a <- a %>% mutate(LS5 = if_else(perc >= 4.5, Var1, "Other"))

others <- a %>% filter(LS5 == "Other")
others <- others [order(others$LS5_WOE),]
binned <- anti_join(a, others, by = "Var1")
binned <- binned %>% mutate(LS5 = LS5_WOE)


others <- others %>% mutate(
  LS5 = case_when(
    LS5_WOE  ==  0.00 ~ "1",                        #this is OK b/c among the zeros there are both events and non-events
    LS5_WOE  >   0.00 ~ "2"
  )
)

a <- others %>% group_by(LS5) %>% summarise(Final = mean(LS5_WOE))
others <- left_join(others, a, by = "LS5")

others <- others %>% mutate(LS5 = Final) %>% select(-Final)

LS5 <- rbind(binned, others)
LS5 <- LS5 %>% select(Var1, LS5) %>% rename(LS5_Combined_WOE = LS5)

physicians <- physicians %>% 
  left_join(LS5, by = c("License_State_5" = "Var1")) %>% 
  relocate(LS5_Combined_WOE, .after = License_State_5)


rm(a)
rm(others)
rm(binned)

physicians <- physicians %>% mutate(
  LS5_Combined_WOE = if_else(is.na(LS5_Combined_WOE), 0, LS5_Combined_WOE),
  LS3_Combined_WOE = if_else(is.na(LS3_Combined_WOE), 0, LS3_Combined_WOE),
  )



#Specialties
physicians <- physicians %>% mutate (Primary_Speciality_Sub = if_else(is.na(Primary_Speciality_Sub), "None", Primary_Speciality_Sub))

train <- physicians%>% filter(set=='train')  
test <- physicians %>% filter(set=='test')

IV <- create_infotables(data = train,
                        y = "Owner")

sub_spec <- print(IV_noValid$Tables$Primary_Speciality_Sub, row.names = F) %>% select (-N, -IV) %>% mutate (Percent = round(Percent * 100, 2))
sub_spec <- sub_spec %>% mutate(Primary_Speciality_Sub = if_else(is.na(Primary_Speciality_Sub), "None", Primary_Speciality_Sub))


sub_spec <- sub_spec %>% mutate(
  Sub_Spec_WOE = case_when(
    WOE  >= -0.30  & WOE  <= -0.28  ~ "1",
    WOE  >= -2.14  & WOE  <= -1.92  ~ "2",
    WOE  >= -1.70  & WOE  <= -0.60  ~ "3",
    WOE  >= -0.10  & WOE  <=  0.00  ~ "4",
    WOE  >=  0.10  & WOE  <=  0.70  ~ "5",
    WOE  >=  0.80  & WOE  <=  3.20  ~ "6"
  ))

a <- sub_spec %>% group_by(Sub_Spec_WOE) %>% summarise(Final = mean(WOE))
sub_spec <- left_join(sub_spec, a, by = "Sub_Spec_WOE") %>%  select(-Sub_Spec_WOE)
sub_spec <- sub_spec  %>% mutate(Sub_Spec_WOE = Final) %>% select(-Final, -Percent, -WOE)


physicians <- physicians %>% 
  left_join(sub_spec, by = c("Primary_Speciality_Sub")) %>% 
  relocate(Sub_Spec_WOE, .after = Primary_Speciality_Sub)

physicians <- physicians %>% mutate(
  Sub_Spec_WOE = if_else(is.na(Sub_Spec_WOE), 0, Sub_Spec_WOE))

rm(IV_noValid)
rm(IV_noValid_Value)


#Number of transactions with top 1 company providing the most money
Num_Trans_w_Top1 <- payments %>% 
  select(Physician_ID, Company_ID, Total_Amount_of_Payment_USDollars) 

Num_Trans_w_Top1 <- Num_Trans_w_Top1 %>% 
  group_by(Physician_ID, Company_ID) %>% 
  mutate(Num_Trans_w_Top1 = n_distinct(Physician_ID)) 

Num_Trans_w_Top1 <- Num_Trans_w_Top1 %>% 
  group_by(Physician_ID, Company_ID) %>% 
  summarise(num = sum(Num_Trans_w_Top1), amount = sum(Total_Amount_of_Payment_USDollars))

Num_Trans_w_Top1 <- Num_Trans_w_Top1 %>% slice_max(amount)
Num_Trans_w_Top1 <- Num_Trans_w_Top1 [-c(2988),]
Num_Trans_w_Top1_for_product <- Num_Trans_w_Top1
Num_Trans_w_Top1 <- Num_Trans_w_Top1  %>% rename(Num_Trans_w_Top1 = num,
                                                 Amount_Trans_from_Top1 = amount) %>% select(-Company_ID)

physicians <- physicians %>% 
  left_join(Num_Trans_w_Top1, by = c("id" = "Physician_ID")) 

rm(Num_Trans_w_Top1)


#Most common related product type with the top 1 company

payments <- payments %>% mutate(
  Product_Type_1 = if_else(is.na(Product_Type_1), "Other",Product_Type_1),
  Product_Type_2 = if_else(is.na(Product_Type_2), "None", Product_Type_2), 
  Product_Type_3 = if_else(is.na(Product_Type_3), "None", Product_Type_3))

payments <- payments %>% mutate(
  Related_Product_Indicator =  case_when(
    Related_Product_Indicator == "Covered" ~ "Yes",
    Related_Product_Indicator == "Non-Covered" ~ "Yes",
    Related_Product_Indicator == "Combination" ~ "Yes",
    Related_Product_Indicator == "Yes" ~ "Yes",
    Related_Product_Indicator == "None" ~ "No",
    Related_Product_Indicator == "No" ~ "No"))

payments <- payments %>% mutate(
  Product_Type_1 = case_when(
    Related_Product_Indicator == "Yes" & Product_Type_1 == "Other" ~ "Other",
    Related_Product_Indicator == "No" & Product_Type_1 == "Other" ~ "None",
    Related_Product_Indicator == "Yes" & Product_Type_1 == "Biological" ~ "Biological",
    Related_Product_Indicator == "Yes" & Product_Type_1 == "Drug" ~ "Drug",
    Related_Product_Indicator == "Yes" & Product_Type_1 == "Drug or Biological" ~ "Drug or Biological",
    Related_Product_Indicator == "Yes" & Product_Type_1 == "Device" ~ "Device",
    Related_Product_Indicator == "Yes" & Product_Type_1 == "Device or Medical Supply" ~ "Device or Medical Supply",
    Related_Product_Indicator == "Yes" & Product_Type_1 == "Medical Supply" ~ "Medical Supply",
    Related_Product_Indicator == "No" & Product_Type_1 == "Biological" ~ "Biological",
    Related_Product_Indicator == "No" & Product_Type_1 == "Drug" ~ "Drug",
    Related_Product_Indicator == "No" & Product_Type_1 == "Drug or Biological" ~ "Drug or Biological",
    Related_Product_Indicator == "No" & Product_Type_1 == "Device" ~ "Device",
    Related_Product_Indicator == "No" & Product_Type_1 == "Device or Medical Supply" ~ "Device or Medical Supply",
    Related_Product_Indicator == "No" & Product_Type_1 == "Medical Supply" ~ "Medical Supply",))


Most_Common_Prod_Type1_w_Top1 <- payments %>% 
  select(Physician_ID, Company_ID, Product_Type_1, Total_Amount_of_Payment_USDollars) 

Most_Common_Prod_Type1_w_Top1 <- Most_Common_Prod_Type1_w_Top1 %>% 
  group_by(Physician_ID, Company_ID) %>% 
  mutate(most_common = n_distinct(Physician_ID)) 

Most_Common_Prod_Type1_w_Top1 <- Most_Common_Prod_Type1_w_Top1 %>% 
  group_by(Physician_ID, Company_ID, Product_Type_1) %>% 
  summarise(num = sum(most_common), amount = sum(Total_Amount_of_Payment_USDollars)) %>% 
  ungroup() %>% 
  group_by (Physician_ID, Company_ID) %>% 
  mutate(total = sum(amount)) 

Highest_Paying_Companies <- Num_Trans_w_Top1_for_product %>% select(-num, -amount)

Most_Common_Prod_Type1_w_Top1 <- Most_Common_Prod_Type1_w_Top1 %>% inner_join(
  Highest_Paying_Companies, by = c("Physician_ID", "Company_ID")) %>% slice_max(num) %>% slice_max(amount)

Most_Common_Prod_Type1_w_Top1 <- Most_Common_Prod_Type1_w_Top1[!duplicated(Most_Common_Prod_Type1_w_Top1$Physician_ID),]
Most_Common_Prod_Type1_w_Top1 <- Most_Common_Prod_Type1_w_Top1 %>% ungroup()

Most_Common_Prod_Type1_w_Top1 <- Most_Common_Prod_Type1_w_Top1 %>% rename(
  Most_Common_Prod_Type1_w_Top1 = Product_Type_1,
  USD_Amount_for_Most_Common_Prod_Type1_w_Top1 = amount) %>% select(
    -num, -total, -Company_ID)

physicians <- physicians %>% 
  left_join(Most_Common_Prod_Type1_w_Top1, by = c("id" = "Physician_ID")) 


#Product type 2
Most_Common_Prod_Type2_w_Top1 <- payments %>% 
  select(Physician_ID, Company_ID, Product_Type_2, Total_Amount_of_Payment_USDollars) 

Most_Common_Prod_Type2_w_Top1 <- Most_Common_Prod_Type2_w_Top1 %>% 
  group_by(Physician_ID, Company_ID) %>% 
  mutate(most_common = n_distinct(Physician_ID)) 

Most_Common_Prod_Type2_w_Top1 <- Most_Common_Prod_Type2_w_Top1 %>% 
  group_by(Physician_ID, Company_ID, Product_Type_2) %>% 
  summarise(num = sum(most_common), amount = sum(Total_Amount_of_Payment_USDollars)) %>% 
  ungroup() %>% 
  group_by (Physician_ID, Company_ID) %>% 
  mutate(total = sum(amount)) 

Most_Common_Prod_Type2_w_Top1 <- Most_Common_Prod_Type2_w_Top1 %>% inner_join(
  Highest_Paying_Companies, by = c("Physician_ID", "Company_ID")
) %>% slice_max(num) %>% slice_max(amount)

Most_Common_Prod_Type2_w_Top1 <- Most_Common_Prod_Type2_w_Top1 %>% ungroup()

Most_Common_Prod_Type2_w_Top1 <- Most_Common_Prod_Type2_w_Top1 %>% rename(
  Most_Common_Prod_Type2_w_Top1 = Product_Type_2) %>% select(
    -num, -total, -Company_ID, -amount)

physicians <- physicians %>% 
  left_join(Most_Common_Prod_Type2_w_Top1, by = c("id" = "Physician_ID")) 


#Product type 3
Most_Common_Prod_Type3_w_Top1 <- payments %>% 
  select(Physician_ID, Company_ID, Product_Type_3, Total_Amount_of_Payment_USDollars) 

Most_Common_Prod_Type3_w_Top1 <- Most_Common_Prod_Type3_w_Top1 %>% 
  group_by(Physician_ID, Company_ID) %>% 
  mutate(most_common = n_distinct(Physician_ID)) 

Most_Common_Prod_Type3_w_Top1 <- Most_Common_Prod_Type3_w_Top1 %>% 
  group_by(Physician_ID, Company_ID, Product_Type_3) %>% 
  summarise(num = sum(most_common), amount = sum(Total_Amount_of_Payment_USDollars)) %>% 
  ungroup() %>% 
  group_by (Physician_ID, Company_ID) %>% 
  mutate(total = sum(amount)) 

Most_Common_Prod_Type3_w_Top1 <- Most_Common_Prod_Type3_w_Top1 %>% inner_join(
  Highest_Paying_Companies, by = c("Physician_ID", "Company_ID")
) %>% slice_max(num) %>% slice_max(amount)

Most_Common_Prod_Type3_w_Top1 <- Most_Common_Prod_Type3_w_Top1 %>% ungroup()

Most_Common_Prod_Type3_w_Top1 <- Most_Common_Prod_Type3_w_Top1 %>% rename(
  Most_Common_Prod_Type3_w_Top1 = Product_Type_3) %>% select(
    -num, -total, -Company_ID, -amount)

physicians <- physicians %>% 
  left_join(Most_Common_Prod_Type3_w_Top1, by = c("id" = "Physician_ID"))


#Proportions Product Types - Not combined 
payments <- payments %>% mutate(
  Product_Type_1 = if_else(Product_Type_1 == "Other", NA_character_, Product_Type_1),
  Product_Type_1 = if_else(Product_Type_1 == "None", NA_character_, Product_Type_1),
  Product_Type_2 = if_else(Product_Type_2 == "None", NA_character_, Product_Type_2), 
  Product_Type_3 = if_else(Product_Type_3 == "None", NA_character_, Product_Type_3)
)


Product_Type_Long <- payments %>% pivot_longer(Product_Type_1:Product_Type_3,
                                               values_to = 'Product_Type')
Product_Type_Long <- prop_wider_by_Physician(Product_Type_Long, Product_Type)

Product_Type_Long <- Product_Type_Long %>% mutate(
  across(.cols = everything(),
         .fns = ~if_else(is.na(.), 0, .))) %>% select(-Product_Type_NA)

physicians <- physicians %>% 
  left_join(Product_Type_Long, by = c("id" = "Physician_ID")) 


physicians_ena <- physicians %>% select(-City, -State, -Zipcode, -Primary_Speciality_Sub, -License_State_1, -License_State_2, -License_State_3, -License_State_4, -License_State_5)

physicians_ena <- physicians_ena %>% mutate(
  across(
    .cols=starts_with('Primary_Speciality'),
    .fns= ~if_else(is.na(.), 'Other', .)))




##################################Final Join###########################

physicians_final <- physicians_col_ata_jonas %>%
  inner_join(physicians_ena, by=c('Physician_ID'='id'))

physicians_final <- physicians_final %>%
  rename(Form_of_Payment_Cash = `Form_of_Payment_or_Transfer_of_Value_Cash or cash equivalent`,
         Form_of_Payment_In_Kind = `Form_of_Payment_or_Transfer_of_Value_In-kind items and services`,
         Number_of_Distinct_Companies = count_distinct_companies,
         Total_Transactions = total_transactions,
         Avg_Days_Between_Payments = avg_days_between_payments,
         Avg_Transactions_Per_Company = avg_transactions_per_company,
         Amount_Payment_from_Top_Company=Amount_Trans_from_Top1,
         Transactions_with_Top_Company=Num_Trans_w_Top1,
         Product_Type_Drug_or_Biological = `Product_Type_Drug or Biological`,
         Product_Type_Device_or_Medical_Supply = `Product_Type_Device or Medical Supply`,
         Product_Type_Medical_Supply = `Product_Type_Medical Supply`)

physicians_final <- physicians_final %>% 
  select(-Third_Party_Covered_NA,
         -Third_Party_Recipient_No,
         -Form_of_Payment_In_Kind,
         -starts_with('Charity'),
         -Product_Type_NA,
         -Avg_Amount_of_Payment,
         -Transactions_in_2013,
         -Nature_of_Payment_Other,
         -Product_Type_Medical_Supply,
         -Owner)


physicians_final <- physicians_final %>% 
  relocate(Ownership_Interest, .after=Product_Type_Drug_or_Biological)                                                

physicians_final <- physicians_final %>%
  mutate_all(~replace_na(.,0))

physicians_final <- physicians_final %>%
  mutate(Ownership_Interest = if_else(set=='test', NA_real_, Ownership_Interest))




# cleaning memory space
rm (a, h, i, j, k, companies_df, physicians_n_best_paying_company, physicians_best_paying_company, companies_n_payments, payments_best_paying_company, payments_w_ownership,Amount_Payments, b,c, Charity, companies, companies_raw, Country_of_Travel_Abroad_Total, d, df, df_filtered, df_train, e,f, Form_of_Payment_or_Transfer_of_Value, g, Highest_Paying_Companies, license_1, license_2, license_3, license_4, license_5, LS1, LS1_sim, LS2, LS2_sim, LS3, LS3_sim, LS4, LS4_sim, LS5, LS5_sim, Most_Common_Prod_Type1_w_Top1, Most_Common_Prod_Type2_w_Top1, Most_Common_Prod_Type3_w_Top1,Num_Payments_Greater_1, Num_State_License, Num_Trans_w_Top1_for_product, Number_of_Local_Companies, Ownership_Interest, payments, payments_df, payments_filtered, payments_jonas, payments_raw, physicians,physicians_col_ata, physicians_col_ata_jonas, physicians_ena, physicians_raw, Product_Type_1, Product_Type_Long, Related_Product_Indicator, specialities, State_of_Travel_Total, Third_Party_Covered, Third_Party_Recipient, Total_Amount_of_Payment_by_Year, train, Transactions_per_Year, Travel_to_Company_Abroad_Country, Travel_to_Company_State, zip, IV, IV_Value, Most_Common_Prod_Cat, Most_Common_Prod_Cat_WOE, sub_spec, test, dd, for_testing, Most_Common_Prod_Cat_Comb_WOE, Ownerrs, Owners, company_transaction_quantile, l)

#######################Training: Random Forest###############################

physicians_final <- physicians_final %>% select (-'Product_Type_Drug/Biological',
                                                 -'Product_Type_Device/Medical Supply')

physicians_final <- physicians_final %>% mutate(
  Ownership_Interest = as.factor(Ownership_Interest),
  Most_Common_Prod_Type1_w_Top1 = as.factor(Most_Common_Prod_Type1_w_Top1),
  Most_Common_Prod_Type2_w_Top1 = as.factor(Most_Common_Prod_Type2_w_Top1),
  Most_Common_Prod_Type3_w_Top1 = as.factor(Most_Common_Prod_Type3_w_Top1),
  Primary_Speciality_General = as.factor(Primary_Speciality_General),
  Primary_Speciality_Main = as.factor(Primary_Speciality_Main)
) 

physicians_final %>% glimpse

train_data <- physicians_final %>% filter(grepl("train", set)) %>% select(-set)
test_data <- physicians_final %>% filter(grepl("test", set)) %>% select(-set)

table(train_data$Ownership_Interest)    #94% NO, 6% YES

folds <- train_data %>% rsample::vfold_cv(v=10, strata = Ownership_Interest)

rec <- recipe(
  Ownership_Interest ~ ., data = train_data) %>% 
  update_role (Physician_ID, new_role = "ID") %>%
  step_other (Primary_Speciality_Main, threshold = 0.02) %>% 
  step_downsample (Ownership_Interest, under_ratio = 1)

rec %>% prep() %>% bake(train_data) %>% glimpse


rf_model <- 
  rand_forest(
    mtry = tune(),
    trees = 500,
    min_n=tune()) %>%
  set_engine("ranger", importance = "impurity") %>%
  set_mode("classification")

rf_workflow <- workflow() %>%
  add_recipe(rec) %>%
  add_model(rf_model)

xgb_params <- parameters(rf_model) %>%
  finalize(train_data)

set.seed(2021)
xgb_grid <- grid_max_entropy(
  xgb_params,
  size=50
)

knitr::kable(head(xgb_grid))

doParallel::registerDoParallel()

xgb_res_grid_b <- tune_grid(
  object = rf_workflow,
  resamples = folds,
  grid=xgb_grid,
  metrics = metric_set(bal_accuracy),
  control = control_grid(verbose=T,
                         allow_par = T,
                         save_pred = T)
)

xgb_res_grid_b %>% collect_metrics() %>% arrange(desc(mean))

res_bayes_undersample <- tune_bayes(
  object = rf_workflow,
  resamples = folds,
  param_info= xgb_params,
  iter = 40,
  metrics = metric_set(bal_accuracy),
  initial = xgb_res_grid_b,
  control = control_bayes(
    verbose=T,
    no_improve=20,
    seed=123
  )
)

res_bayes_undersample %>% collect_metrics() %>% arrange(desc(mean))

#Get best config out of  cross validation
rf_best_config <- res_bayes_undersample %>% select_best('bal_accuracy')

#Reproduce best model
final_workflow <- rf_workflow %>% 
  finalize_workflow(rf_best_config)

trained_rf_model <- final_workflow %>% 
  fit(data=train_data)

#Create Predictions on training set
train_set_with_predictions <-
  bind_cols(
    train_data,
    trained_rf_model %>% predict(train_data)
  )

conf_matrix <- train_set_with_predictions %>% 
  conf_mat(truth = Ownership_Interest, estimate = .pred_class)

conf_matrix

trained_rf_model %>% pull_workflow_fit() %>% vip::vip()


submission <- bind_cols(
  test_data %>% select(Physician_ID),
  trained_rf_model %>% predict(test_data)
) %>% 
  # column names must be the same as in submission template!
  rename(prediction = .pred_class, id = Physician_ID) %>% 
  # order by id
  arrange(id)

predictions_rf <- submission$prediction


########################################   XGB    ###########################################
physicians_final_xgb <- physicians_final %>% mutate(Ownership_Interest=as.numeric(Ownership_Interest)-1)

IV <- create_infotables(data = physicians_final_xgb %>% filter(set=='train'),
                        y = "Ownership_Interest")

gen_spec <- print(IV$Tables$Primary_Speciality_General, row.names = F) %>% select (-IV) %>% mutate (Percent = round(Percent * 100, 2))

gen_spec <- gen_spec %>% mutate(
  Gen_Spec_WOE = case_when(
    WOE  >= -0.40  & WOE  <= -0.30  ~ "1",
    WOE  >= -0.10  & WOE  <=  0.10  ~ "2",
    WOE  >=  0.20  & WOE  <=  3.00  ~ "3"
  ))

gen_spec <- gen_spec %>% 
  group_by(Gen_Spec_WOE) %>% 
  mutate(Final = case_when(
    Gen_Spec_WOE == "1" ~ mean(WOE),
    Gen_Spec_WOE == "2" ~ mean(WOE),
    Gen_Spec_WOE == "3" ~ 0.9029)) %>% 
  mutate(Gen_Spec_WOE = Final) %>% 
  select(-Final, -Percent, -WOE, -N)

physicians_final_xgb <- physicians_final_xgb %>% 
  left_join(gen_spec, by = c("Primary_Speciality_General")) %>% 
  relocate(Gen_Spec_WOE, .after = Primary_Speciality_General) %>% 
  select (-Primary_Speciality_General)


# Primary Specialty Main
main_spec <- print(IV$Tables$Primary_Speciality_Main, row.names = F) %>% select (-IV) %>% mutate (Percent = round(Percent * 100, 2))

main_spec <- main_spec %>% mutate(
  Main_Spec_WOE = case_when(
    WOE  >= -3.00  & WOE  <= -2.20  ~ "1",
    WOE  >= -1.90  & WOE  <= -1.30  ~ "2",
    WOE  >= -1.00  & WOE  <= -0.70  ~ "3",
    WOE  >=  0.00  & WOE  <=  0.06  ~ "4",
    WOE  >=  0.10  & WOE  <=  0.26  ~ "5",
    WOE  >=  0.29  & WOE  <=  0.46  ~ "6",
    WOE  >=  0.50  & WOE  <=  0.90  ~ "7",
    WOE  >=  1.00  & WOE  <=  3.00  ~ "8"
  )) %>% 
  group_by(Main_Spec_WOE) %>% 
  mutate(Final = mean(WOE)) %>% 
  mutate(Main_Spec_WOE = Final) %>% 
  select(-Final, -Percent, -WOE, -N)

physicians_final_xgb <- physicians_final_xgb %>% 
  left_join(main_spec, by = c("Primary_Speciality_Main")) %>% 
  relocate(Main_Spec_WOE, .after = Primary_Speciality_Main) %>% 
  select (-Primary_Speciality_Main)

physicians_final_xgb <- physicians_final_xgb %>% mutate(
  Main_Spec_WOE = if_else(is.na(Main_Spec_WOE), 0.210322484, Main_Spec_WOE))


#top_company_legal_form 
top_company_legal_form_WOE  <- print(IV$Tables$top_company_legal_form, row.names = F) %>% 
  select (-IV, -Percent, -N) %>% 
  mutate(top_company_legal_form_WOE = WOE) %>% 
  select (-WOE)

physicians_final_xgb <- physicians_final_xgb %>% 
  left_join(top_company_legal_form_WOE, by = c("top_company_legal_form")) %>% 
  relocate(top_company_legal_form_WOE, .after = top_company_legal_form) %>% 
  select (-top_company_legal_form)

#Most_Common_Prod_Type1_w_Top1  
physicians_final_xgb <- physicians_final_xgb %>% 
  mutate (Most_Common_Prod_Type1_w_Top1 = 
            as.factor(if_else (Most_Common_Prod_Type1_w_Top1 == "Medical Supply", "Device or Medical Supply", 
                               as.character(Most_Common_Prod_Type1_w_Top1))))

train <- physicians_final_xgb %>% filter(set=='train')  
IV <- create_infotables(data = train, y = "Ownership_Interest")

Most_Common_Prod_Type1_w_Top1_WOE <- print(IV$Tables$Most_Common_Prod_Type1_w_Top1, row.names = F) %>% 
  mutate (Percent = round(Percent * 100, 2)) %>% 
  select (-IV, -Percent, -N) %>% 
  mutate(Most_Common_Prod_Type1_w_Top1_WOE = WOE) %>% 
  select (-WOE)

physicians_final_xgb <- physicians_final_xgb %>% 
  left_join(Most_Common_Prod_Type1_w_Top1_WOE, by = c("Most_Common_Prod_Type1_w_Top1")) %>% 
  relocate(Most_Common_Prod_Type1_w_Top1_WOE, .after = Most_Common_Prod_Type1_w_Top1) %>% 
  select (-Most_Common_Prod_Type1_w_Top1)

#Most_Common_Prod_Type2_w_Top1  
Most_Common_Prod_Type2_w_Top1_WOE <- print(IV$Tables$Most_Common_Prod_Type2_w_Top1, row.names = F) %>% 
  mutate (Percent = round(Percent * 100, 2)) %>% 
  select (-IV) 

Most_Common_Prod_Type2_w_Top1_WOE <- Most_Common_Prod_Type2_w_Top1_WOE  %>% mutate(
  Most_Common_Prod_Type2_w_Top1_WOE  = case_when(
    WOE  >= -1.30  & WOE  <= -0.80  ~ "1",
    WOE  >=  0.00  & WOE  <=  0.03  ~ "2",
    WOE  >=  0.25  & WOE  <=  0.40  ~ "3"
  )) %>% 
  group_by(Most_Common_Prod_Type2_w_Top1_WOE ) %>% 
  mutate(Final = mean(WOE)) %>% 
  mutate(Most_Common_Prod_Type2_w_Top1_WOE  = Final) %>% 
  select(-Final, -Percent, -WOE, -N)

physicians_final_xgb <- physicians_final_xgb %>% 
  left_join(Most_Common_Prod_Type2_w_Top1_WOE, by = c("Most_Common_Prod_Type2_w_Top1")) %>% 
  relocate(Most_Common_Prod_Type2_w_Top1_WOE, .after = Most_Common_Prod_Type2_w_Top1) %>% 
  select (-Most_Common_Prod_Type2_w_Top1)


#Most_Common_Prod_Type3_w_Top1
Most_Common_Prod_Type3_w_Top1_WOE <- print(IV$Tables$Most_Common_Prod_Type3_w_Top1, row.names = F) %>% 
  mutate (Percent = round(Percent * 100, 2)) %>% 
  select (-IV) 

Most_Common_Prod_Type3_w_Top1_WOE <- Most_Common_Prod_Type3_w_Top1_WOE  %>% mutate(
  Most_Common_Prod_Type3_w_Top1_WOE  = case_when(
    WOE  >= -0.80  & WOE  <= -0.0001  ~ "1",
    WOE  >=  0.00  & WOE  <=  3.00  ~ "2"
  )) %>% 
  group_by(Most_Common_Prod_Type3_w_Top1_WOE ) %>% 
  mutate(Final = weighted.mean(WOE, N)) %>% 
  mutate(Most_Common_Prod_Type3_w_Top1_WOE  = Final) %>% 
  select(-Final, -Percent, -WOE, -N)

physicians_final_xgb <- physicians_final_xgb %>% 
  left_join(Most_Common_Prod_Type3_w_Top1_WOE, by = c("Most_Common_Prod_Type3_w_Top1")) %>% 
  relocate(Most_Common_Prod_Type3_w_Top1_WOE, .after = Most_Common_Prod_Type3_w_Top1) %>% 
  select (-Most_Common_Prod_Type3_w_Top1)

rm(gen_spec, m, IV, main_spec, Most_Common_Prod_Type1_w_Top1_WOE, Most_Common_Prod_Type2_w_Top1_WOE, Most_Common_Prod_Type3_w_Top1_WOE, test, train, top_company_legal_form_WOE)
physicians_final %>% dfSummary() %>% view()


####################    XGB:Training        ###################
physicians_final_xgb <- physicians_final_xgb %>% mutate(
  Ownership_Interest = as.factor(Ownership_Interest)
) 

train_xgb <- physicians_final_xgb %>% filter(set=='train') %>% select(-set)
test_xgb <- physicians_final_xgb %>% filter(set=='test') %>% select(-set)

folds_xgb <- train_xgb %>% rsample::vfold_cv(v=10, strata = Ownership_Interest)

rec_xgb <- recipe(
  Ownership_Interest ~ ., data = train_xgb) %>% 
  update_role (Physician_ID, new_role = "ID") %>% 
  themis::step_downsample(Ownership_Interest, under_ratio = 1)

rec_xgb %>% prep() %>% bake(train_xgb) %>% glimpse

xgb_model_b <- 
  boost_tree(
    mode = 'classification',
    learn_rate = tune(),
    tree_depth = tune(),
    min_n = tune(),
    loss_reduction = tune(), 
    trees = 500) %>%
  set_engine("xgboost")


xgb_wf_b <- workflow() %>%
  add_recipe(rec_xgb) %>%
  add_model(xgb_model_b)

xgb_params <- parameters(
  min_n(),
  tree_depth(),
  learn_rate(),
  loss_reduction()
)


xgb_grid <- grid_max_entropy(
  xgb_params,
  size=50
)
knitr::kable(head(xgb_grid))

all_cores <- parallel::detectCores(logical = FALSE)
library(doParallel)
cl <- makePSOCKcluster(all_cores)
registerDoParallel(cl)
registerDoParallel()

xgb_res_grid_b <- tune_grid(
  object = xgb_wf_b,
  resamples = folds_xgb,
  grid=xgb_grid,
  metrics = metric_set(bal_accuracy),
  control = control_grid(verbose=T,
                         allow_par = T,
                         save_pred = T)
)

xgb_res_grid_b %>% collect_metrics() %>% arrange(desc(mean))

# xgb_res_bayes_b <- tune_bayes(
#   object = xgb_wf_b,
#   resamples = folds_xgb,
#   metrics = metric_set(bal_accuracy),
#   initial=xgb_res_grid_b,
#   iter = 40,
#   control = control_bayes(verbose=T,
#                           no_improve=20,
#                           seed=2021,
#                           save_pred = T))
# 
# xgb_res_grid_b %>% collect_metrics() %>% arrange(desc(mean))   #0.810, 2nd run with min_n tuning 80.4

xgb_res_grid_b_best <- xgb_res_grid_b %>% select_best()

######Finalize the Model#########
xgb_wf_b_final <- xgb_wf_b %>% 
  finalize_workflow(xgb_res_grid_b_best)

xgb_b_final_fit <- xgb_wf_b_final %>% 
  fit(data=train_xgb)

xgb_b_final_fit %>% pull_workflow_fit() %>% vip(geom='col', num_features = ncol(train_xgb))


confMatrix_xgb_b <- train_xgb %>% 
  bind_cols(xgb_b_final_fit %>% 
              predict(train_xgb)) %>% 
  conf_mat(truth=Ownership_Interest, 
           estimate = .pred_class)

confMatrix_xgb_b


submission <- bind_cols(
  test_xgb %>% select(Physician_ID),
  xgb_b_final_fit  %>% predict(test_xgb)
) %>% 
  # column names must be the same as in submission template!
  rename(prediction = .pred_class, id = Physician_ID) %>% 
  # order by id
  arrange(id)

predictions_xgb <- submission$prediction




################   Combine Predictions    ################################

bind_models <- function(pred1, pred2) {
  # pred1 <- enquo(pred1)
  # pred2 <- enquo(pred2)
  pred_combined <- pred1 %>% bind_cols(pred2) %>% 
    mutate(pred_combined = case_when(
      pred1 == 1 & pred2 == 1 ~ 1,
      TRUE ~ 0
    )) %>% select(pred_combined)
}

pred_combined <- bind_models(predict(xgb_b_final_fit, train_xgb), predict(trained_rf_model, train_data))
truth <- as.tibble(train_xgb$Ownership_Interest)
pred_combined$pred_combined <- as.factor(pred_combined$pred_combined)

pred_combined %>% bind_cols(truth) %>% conf_mat(truth=value, estimate=pred_combined) 

pred_combined %>% bind_cols(truth) %>% bal_accuracy(value, pred_combined)

pred_combined_test <- bind_models(pred1 = predictions_rf,pred2 = predictions_xgb)

submission <- bind_cols(
  test_data %>% select(Physician_ID),
  pred_combined_test) %>% 
  rename(prediction = pred_combined, id = Physician_ID) %>% 
  # order by id
  arrange(id)


write_csv(submission, "submissions/predictions_EAJ21_9.csv")


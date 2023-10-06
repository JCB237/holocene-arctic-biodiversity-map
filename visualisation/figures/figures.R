library(ggplot2)
library(ggridges)
library(purrr)
library(tidyr)
library(dplyr)
library(stringr)

theme_set(theme_minimal())

site_data <- tibble(read.csv("../thalloo-static-site/map-data/ahbdb_sites.csv"))

site_data_split <-
  site_data %>%
  separate_rows(proxy_category, sep = ";") %>%
  mutate(proxy_category = as.factor(proxy_category)) %>%
  mutate(proxy_category = str_replace(proxy_category, 'OtherProxy \\(ShortText \\"', "")) %>%
  mutate(proxy_category = str_replace(proxy_category, '\\"\\)\\)', "")) %>%    
  mutate(proxy_category = str_replace(proxy_category, '\\"\\)', "")) %>%
  mutate(proxy_category = str_replace(proxy_category, 'Microfossil \\(OtherMicrofossilGroup \\(ShortText \\"', "")) %>%
  mutate(proxy_category = str_replace(proxy_category, 'OtherMicrofossilGroup \\(ShortText \\"', "")) %>%
  mutate(proxy_category = str_replace(proxy_category, 'Acritarch', "Other")) %>% 
  mutate(proxy_category = str_replace(proxy_category, 'Suberised Basal Cells', "Other")) %>% 
  mutate(proxy_category = str_replace(proxy_category, 'Palynomorph', "Other")) %>% 
  mutate(proxy_category = str_replace(proxy_category, 'Contemporary Mammals', "Other")) %>% 
  mutate(proxy_category = str_replace(proxy_category, 'Trichoblast', "Other")) %>% 
  mutate(proxy_category = str_replace(proxy_category, 'Colony', "Other")) %>% 
  mutate(proxy_category = str_replace(proxy_category, 'Bosmina', "Cladocera")) %>% 
  mutate(proxy_category = str_replace(proxy_category, 'Fungal Spore', "Spore")) %>%
  mutate(proxy_category = str_replace(proxy_category, 'Microfossil ', "")) %>%
  mutate(proxy_category = str_replace(proxy_category, 'Head Capsule', "Chironomid")) %>%
  mutate(proxy_category = str_replace(proxy_category, 'Megafossil', "Mega- or Sub-fossil")) %>%
  mutate(proxy_category = str_replace(proxy_category, 'Macrofossil', "Mega- or Sub-fossil")) %>%
  mutate(proxy_category = str_replace(proxy_category, 'Fossil Plants', "Mega- or Sub-fossil")) %>%
  mutate(proxy_category = str_replace(proxy_category, 'Fossil Mammals', "Mega- or Sub-fossil")) %>%
  mutate(proxy_category = str_replace(proxy_category, 'PlantMega- or Sub-fossil', "Plant Macrofossil")) %>%
  mutate(proxy_category = str_replace(proxy_category, 'Cladoceran', "Cladocera")) %>%
  filter(proxy_category != "")


site_data_individual_years <-
  site_data_split %>%
  filter(!is.na(latest_extent)) %>% filter(!is.na(earliest_extent)) %>%
  mutate(year = map2(latest_extent, earliest_extent, seq)) %>%
  unnest(year)

# 1. Proxy types by temporal extent
ggplot(site_data_individual_years, aes(x = year, color = proxy_category, group = proxy_category)) + 
  #geom_density_ridges(fill = "#00AFBB", rel_min_height = 0.005, stat = "binline", scale = 0.8) +
  geom_density(aes(y =after_stat(count))) +
  xlim(-70, 11000) +
  xlab("Year (Calendar Years Before Present") +
  ylab("Proxy Category") +
  scale_x_continuous(breaks = round(seq(0, 11000, by = 1000))) +
  facet_wrap(~sample_origin, ncol = 1, scales = "free_y")

ggplot(site_data_individual_years, aes(x = year, y = proxy_category, group = proxy_category)) + 
  geom_density_ridges(fill = "#00AFBB", rel_min_height = 0.005) +
  xlim(-70, 11000) +
  ylab("Year (Calendar Years Before Present") +
  xlab("Proxy Category") +
  scale_x_continuous(breaks = round(seq(0, 11000, by = 1000)))

ggplot(site_data_individual_years) + 
  geom_bar(aes(x = year, fill = sample_origin), position = "fill") +
  scale_fill_viridis_d() +
  xlim(-70, 11000) +
  xlab("Year (Calendar Years Before Present") +
  ylab("Sample Origin")

# 1. CHELSA temperature variability vs latitude
ggplot(site_data_split %>% filter(variability_temp > 0.05), aes(x = variability_temp, y = earliest_extent)) +
  geom_point(color = "black", size = 0.75) +
  stat_density_2d(alpha = 0.5, aes(fill = ..level..), geom = "polygon") +
  xlab("Air temperature (variability during time-series)") +
  ylab("Latitude (decimal degrees)") +
  facet_wrap(~sample_origin, ncol = 1) +
  scale_fill_viridis_c()


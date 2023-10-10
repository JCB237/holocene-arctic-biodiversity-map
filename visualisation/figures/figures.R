library(ggplot2)
library(ggridges)
library(purrr)
library(tidyr)
library(dplyr)
library(stringr)
library(ggthemes)

quartz(width = 5, height = 5)

# https://rpubs.com/Koundy/71792
theme_publication <- function(base_size=20, base_family="helvetica") {
  (ggthemes::theme_foundation()
    + theme(plot.title = element_text(face = "bold",
                                      size = rel(1.2), hjust = 0.5),
            text = element_text(),
            panel.background = element_rect(colour = NA),
            plot.background = element_rect(colour = NA),
            panel.border = element_rect(colour = NA),
            axis.title = element_text(face = "bold",size = rel(1)),
            axis.title.y = element_text(angle=90,vjust =2),
            axis.title.x = element_text(vjust = -0.2),
            axis.text = element_text(), 
            axis.line = element_line(colour="black"),
            axis.ticks = element_line(),
            panel.grid.major = element_line(colour="#f0f0f0"),
            panel.grid.minor = element_blank(),
            legend.key = element_rect(colour = NA),
            legend.position = "bottom",
            legend.direction = "horizontal",
            legend.key.size= unit(0.2, "cm"),
            legend.margin = margin(t = 0, unit='cm'),
            legend.title = element_text(face="italic"),
            plot.margin = unit(c(10,5,5,5),"mm"),
            strip.background = element_rect(colour="#f0f0f0",fill="#f0f0f0"),
            strip.text = element_text(face="bold")
    ))
}

scale_fill_publication <- function(...){
  discrete_scale("fill","Publication",scales::manual_pal(values = c("#386cb0","#fdb462","#7fc97f","#ef3b2c","#662506","#a6cee3","#fb9a99","#984ea3","#ffff33")), ...)
}

scale_colour_publication <- function(...){
  discrete_scale("colour","Publication",scales::manual_pal(values = c("#386cb0","#fdb462","#7fc97f","#ef3b2c","#662506","#a6cee3","#fb9a99","#984ea3","#ffff33")), ...)
}

site_data <- tibble(read.csv("visualisation/thalloo-static-site/map-data/ahbdb_sites.csv"))

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


# 1. What have we captured beyond existing databases?
site_data_not_db <-
  site_data_split %>%
  filter(source_type != "Database entry") %>%
  filter(inferred_as != "Placeholder from NeotomaDB Import [Kingdom]")

# Number of additional time-series by category
additional_by_type <- 
  site_data_not_db %>%
  group_by(proxy_category) %>%
  summarise(n = n())

ggplot(additional_by_type, aes(y = proxy_category, x = n)) +
  geom_col() +
  theme_publication()

# Co-occurrence of authorship
# TODO use a cooccur network analysis to plot this.
additional_by_author_occ <-
  tibble(name =
    site_data_not_db %>%
    mutate(author = str_split(source_authors, ";")) %>%
    pull(author) %>%
    unlist()) %>%
  group_by(name) %>%
  summarise(n = n()) %>%
  arrange(desc(n)) %>%
  top_n(27)

ggplot(additional_by_author_occ, aes(y = name, x = n)) +
  geom_col() +
  theme_publication()

# 2. Biodiversity questions

# Measures by spatial resolution
# TODO needs original spatial extent

# Measures by temporal resolution
# TODO needs dates

# Taxonomic resolution - does proxy method influence
# taxonomic resolution?
additional_by_taxon_res <-
  site_data_not_db %>%
  mutate(rank = str_extract_all(inferred_as, "\\[(.*?)\\]")) %>%
  select(proxy_category, rank) %>%
  unnest(rank) %>%
  group_by(rank) %>%
  summarise(n = n())

ggplot(additional_by_taxon_res, aes(x = proxy_category, fill = rank)) +
  geom_bar() +
  theme_publication() +
  scale_x_discrete(guide = guide_axis(n.dodge = 2)) +
  scale_fill_publication()

# Assemblage size
additional_assemblage_n <-
  site_data_not_db %>%
  mutate(rank = str_extract_all(inferred_as, "\\[(.*?)\\]")) %>%
  mutate(family = str_split(taxon_family, ";"),
         genus = str_split(taxon_genus, ";"),
         species = str_split(taxon_species, ";")) %>%
  mutate(family = map(family, unique),
         genus = map(genus, unique),
         species = map(species, unique)) %>%
  mutate(n = map_dbl(rank, length),
         n_family = map_dbl(family, length),
         n_genus = map_dbl(genus, length),
         n_species = map_dbl(species, length))

ggplot(additional_assemblage_n, aes(x = n, fill = proxy_category)) +
  geom_histogram(binwidth = 5) +
  theme_publication()

ggplot(additional_assemblage_n, aes(x = n, y = n_species)) +
  geom_point() +
  geom_smooth()

additional_assemblage_n %>%
filter(proxy_category != "Contemporaneous Whole Organism") %>%
pull(n) %>%
summary()

# Assemblage size by family

ggplot(additional_assemblage_n, aes(x = n, fill = proxy_category)) +
  geom_histogram(binwidth = 5) +
  theme_publication()



# Inference methods - what are they?
additional_by_infer <-
  site_data_not_db %>%
  mutate(infer = strsplit(inferred_using, ";")) %>%
  select(proxy_category, infer) %>%
  unnest(infer) %>%
  group_by(infer) %>%
  summarise(n = n())

# Are cores dated throughout their temporal extent?

# Display bars with the 'extrapolated' bottom, and
# the dated middle section.
oldest_extrapolation <-
  site_data_not_db %>%
  filter(!is.na(depth_max)) %>%
  filter(dating_levels != "") %>%
  mutate(levels = map(str_split(dating_levels, ";"), as.numeric)) %>%
  mutate(max_date_level = map_dbl(levels, max),
         timeline_id = seq(1, nrow(.))) %>%
  filter(source_id != "sourcenode_pub_craig_tfptalqgrica_1988") %>%

individual_dates <- 
  unnest(oldest_extrapolation, levels) %>%
  select(level = levels,
         timeline_id)

ggplot(oldest_extrapolation %>% arrange(depth_max)) +
  geom_linerange(color = "black", mapping = aes(x = timeline_id, ymin = max_date_level, ymax = depth_max, lwd = 1)) +
  geom_linerange( aes(x = timeline_id, ymin = depth_min, ymax = max_date_level, lwd = 1, color = sample_origin)) +
  geom_point(shape = 4, data = individual_dates, aes(x = timeline_id, y = level)) +
  scale_y_reverse() +
  theme_publication() +
  ylab("Depth (cm)") +
  scale_colour_publication()


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


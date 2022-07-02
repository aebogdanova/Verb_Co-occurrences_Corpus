import re
import random
import tqdm
import json

with open('data/all.json', encoding='utf-8') as file:
    all_genres = json.load(file)
with open('data/science.json', encoding='utf-8') as file:
    science = json.load(file)
with open('data/fiction.json', encoding='utf-8') as file:
    fiction = json.load(file)
with open('data/news.json', encoding='utf-8') as file:
    news = json.load(file)
with open('data/wiki.json', encoding='utf-8') as file:
    wiki = json.load(file)
    
genres_stats = [fiction, news, science, wiki, all_genres]
genres_names = ['fiction', 'news', 'science', 'wiki', 'all']

# сохраняем исходную информацию
initial = {}
for name, stat in zip(genres_names, genres_stats):
    initial[name] = {
        'verbs': len(stat['verbs']),
        'prepositions': len(stat['prepositions']),
        'nouns': len(stat['nouns']),
        'combinations': len(stat['combinations'])
    }

# сюда будем сохранять количество отфильтрованных токенов
filtered = {}

initial_json = json.dumps(initial, ensure_ascii=False)
with open('data/initial.json', 'w', encoding='utf-8') as file:
    file.write(initial_json) 
    
# уберем '\xad' из всех списков
def clean_xad(freq_dict):
    incorrect = [item for item in freq_dict if '\xad' in item]
    for item in incorrect:
        correct = item.replace('\xad', '')
        if correct in freq_dict:
            freq_dict[correct] += freq_dict[item]
        else:
            freq_dict[correct] = freq_dict[item]
        freq_dict.pop(item)
    return freq_dict
    
for name, stat in zip(genres_names, genres_stats):

    print(name)
    print('-'*30)

    verbs_n = len(stat['verbs'])
    stat['verbs'] = clean_xad(stat['verbs'])
    print('Verbs:', len(stat['verbs'])/verbs_n)

    preps_n = len(stat['prepositions'])
    stat['prepositions'] = clean_xad(stat['prepositions'])
    print('Prepositions:', len(stat['prepositions'])/preps_n)

    nouns_n = len(stat['nouns'])
    stat['nouns'] = clean_xad(stat['nouns'])
    print('Nouns:', len(stat['nouns'])/nouns_n)

    combs_n = len(stat['combinations'])
    stat['combinations'] = clean_xad(stat['combinations'])
    print('Combinations:', len(stat['combinations'])/combs_n)
    print('\n')

    filtered[name] = {
        'verbs': verbs_n - len(stat['verbs']),
        'prepositions': preps_n - len(stat['prepositions']),
        'nouns': nouns_n - len(stat['nouns']),
        'combinations': combs_n - len(stat['combinations'])
    }
    
# фильтрация глаголов
verbs = all_genres['verbs'].keys()

def out(func, sample_size=30):
    def wrapper(tokens):
        res = func(tokens)
        print(f'Filtered: {len(res)} ({round(len(res)/len(tokens)*100, 3)}%)')
        print('Examples:', str(random.sample(res, sample_size)))
        return res
    return wrapper
    
# (1) глаголы, которые оканчиваются на "-ый", "-ий", "-ой"
@out
def filter_flexion(verbs):
    return [verb for verb in verbs if verb[-2:] in ['ый', 'ий', 'ой']]

filtered_flexion = filter_flexion(verbs)


# (2) глаголы, в которых есть хотя бы один символ, не принадлежащий [а-яА-ЯёЁ_-]
@out
def filter_symbols(verbs):
    return [verb for verb in verbs if re.findall('[^а-яА-ЯёЁ_-]', verb)]

filtered_symbols = filter_symbols(verbs)

# (3) глаголы с "ё", которые в случае замены "ё" на "е" не образуют новый глагол (те глаголы, у которых при замене нет аналога с "е", оставляем)
@out
def filter_yo(verbs):
    return [verb for verb in verbs if (('ё' in verb) and (re.sub('ё', 'е', verb) in verbs))]

filtered_yo = filter_yo(verbs)

# (4) глаголы с несколькими "не_"
@out
def filter_ne(verbs):
    return [verb for verb in verbs if re.findall('(не_){2,}', verb)]

filtered_ne = filter_ne(verbs)

# фильтруем глаголы по (1)-(4) фильтрам
filtered_1234 = set(filtered_flexion + filtered_symbols + filtered_yo + filtered_ne)

for name, stat in zip(genres_names, genres_stats):
    verbs_n = len(stat['verbs'])
    stat['verbs'] = dict(filter(lambda x: x[0] not in filtered_1234, stat['verbs'].items()))
    print(name)
    print('-'*30)
    print('Vebrs:', len(stat['verbs']) / verbs_n)
    print('\n')
    filtered[name]['verbs'] += verbs_n - len(stat['verbs'])

verbs = all_genres['verbs'].keys()

# (5) глаголы, для которых pymorphy не приводит ни одного разбора с глагольным pos-тэгом
import pymorphy2
from pymorphy2 import MorphAnalyzer
pymorphy = MorphAnalyzer()

@out
def filter_pymorphy(verbs):
    filtered = []
    for verb in verbs:
        lemma = verb
        if 'не_' in verb:
            lemma = verb[3:]
        if not list(set([i.tag.POS for i in pymorphy.parse(lemma)]) & set(['VERB', 'INFN'])):
            filtered.append(verb)
    return filtered

filtered_pymorphy = filter_pymorphy(verbs)

# (6) глаголы, которых нет в opencorpora
@out
def filter_verbs_opencorpora(verbs):
    filtered = []
    for verb in tqdm.tqdm(verbs):
        lemma = verb
        if 'не_' in verb:
            lemma = verb[3:]
        parse = [i for i in pymorphy.parse(lemma)]
        norm = []
        for i in parse:
            if type(i.methods_stack[0][0]) == pymorphy2.units.by_lookup.DictionaryAnalyzer:
                if len(i.methods_stack) > 1:
                    if type(i.methods_stack[1][0]) != pymorphy2.units.by_analogy.UnknownPrefixAnalyzer:
                        norm.append(i)
                else:
                    norm.append(i)
        if not norm:
            filtered.append(verb)
    return filtered

filtered_verbs_opencorpora = filter_verbs_opencorpora(verbs)

# фильтруем глаголы по (5)-(6) фильтрам
filtered_56 = set(filtered_pymorphy + filtered_verbs_opencorpora)
for name, stat in zip(genres_names, genres_stats):
    verbs_n = len(stat['verbs'])
    stat['verbs'] = dict(filter(lambda x: x[0] not in filtered_56, stat['verbs'].items()))
    print(name)
    print('-'*30)
    print('Verbs:', len(stat['verbs']) / verbs_n)
    print('\n')
    filtered[name]['verbs'] += verbs_n - len(stat['verbs'])

verbs = all_genres['verbs'].keys()

# (7) глаголы, частота которых во всем корпусе 3 и меньше (только для глаголов без "не")
@out
def filter_freq(verbs):
    return [verb for verb in verbs if ('не_' not in verb) and (all_genres['verbs'][verb] <= 3)]

filtered_frequency = filter_freq(verbs)

for name, stat in zip(genres_names, genres_stats):
    verbs_n = len(stat['verbs'])
    stat['verbs'] = dict(filter(lambda x: x[0] not in filtered_frequency, stat['verbs'].items()))
    print(name)
    print('-'*30)
    print('Verbs:', len(stat['verbs']) / verbs_n)
    print('\n')
    filtered[name]['verbs'] += verbs_n - len(stat['verbs'])
verbs = all_genres['verbs'].keys()

# сохраняем текущую информацию об количестве отфильтрованных глаголов
filtered_json = json.dumps(filtered, ensure_ascii=False)
with open('data/_filtered.json', 'w', encoding='utf-8') as file:
    file.write(filtered_json)
    
# фильтрация сочетаний
def filter_combinations(name, stat):
    combinations_incorrect = {}
    combinations_new = {}
    verbs = stat['verbs'].keys()
    for comb in tqdm.tqdm(stat['combinations']):
        verb, preposition, noun = comb.split('__')[:3]
        parse_noun = [i for i in pymorphy.parse(noun)]
        norm_noun = []
        for i in parse_noun:
            if type(i.methods_stack[0][0]) == pymorphy2.units.by_lookup.DictionaryAnalyzer:
                if len(i.methods_stack) > 1:
                    if type(i.methods_stack[1][0]) != pymorphy2.units.by_analogy.UnknownPrefixAnalyzer:
                        norm_noun.append(i)
                else:
                    norm_noun.append(i)
        preposition_split = preposition.split()
        norm_preposition = []
        for token in preposition_split:
            if token == 'NO':
                norm_preposition.append(token)
            else:
                parse_token = [i for i in pymorphy.parse(token)]
                norm_token = []
                for i in parse_token:
                    if type(i.methods_stack[0][0]) == pymorphy2.units.by_lookup.DictionaryAnalyzer:
                        if len(i.methods_stack) > 1:
                            if type(i.methods_stack[1][0]) != pymorphy2.units.by_analogy.UnknownPrefixAnalyzer:
                                norm_token.append(i)
                        else:
                            norm_token.append(i)
                if norm_token:
                    norm_preposition.append(token)
        if norm_noun and (len(preposition_split) == len(norm_preposition)) and (verb in verbs):
            combinations_new[comb] = stat['combinations'][comb]
        else:
            combinations_incorrect[comb] = stat['combinations'][comb]
    json_filtered = json.dumps(combinations_new, ensure_ascii=False)
    with open('data/final/'+name+'_final.json', 'w', encoding='utf-8') as file:
        file.write(json_filtered)
    json_incorrect = json.dumps(combinations_incorrect, ensure_ascii=False)
    with open('data/incorrect/'+name+'_incorrect.json', 'w', encoding='utf-8') as file:
        file.write(json_incorrect)
        
for name, stat in zip(genres_names, genres_stats):
    filter_combinations(name, stat)

# gреобразуем получившиеся сочетания в формат словаря
def transform_and_save(name, combinations):
    transformed = {}
    for combination in tqdm.tqdm(combinations):
        # split into verb, preposition, noun and features
        verb, prep, noun, case, num, anim, rel = combination.split('__')
        feats = case+'__'+num+'__'+anim+'__'+rel
        # verbs
        if verb not in transformed:
            transformed[verb] = [0, {}]
        transformed[verb][0] += combinations[combination]
        # prepositions
        if prep not in transformed[verb][1]:
            transformed[verb][1][prep] = [0, {}]
        transformed[verb][1][prep][0] += combinations[combination]
        # grammar features
        if feats not in transformed[verb][1][prep][1]:
            transformed[verb][1][prep][1][feats] = [0, {}]
        transformed[verb][1][prep][1][feats][0] += combinations[combination]
        # nouns
        if noun not in transformed[verb][1][prep][1][feats][1]:
            transformed[verb][1][prep][1][feats][1][noun] = 0
        transformed[verb][1][prep][1][feats][1][noun] += combinations[combination]
    object_json = json.dumps(transformed, ensure_ascii=False)
    with open('data/final'+name+'_transformed.json', 'w', encoding='utf-8') as file:
        file.write(object_json)
import os
import tqdm
import json
from collections import Counter, defaultdict
from minio import Minio
import conllu

class Statistics:

    ACCESS_KEY = ''
    SECRET_KEY = ''
    DIR_CONLLU = ''
    DIR_JSON = ''

    def __init__(self):
        # Initializing Minio client object
        self.minioClient = Minio(
            'cosyco.ru:9000',
            access_key=self.ACCESS_KEY,
            secret_key=self.SECRET_KEY,
            secure=False
            )
        # List of Minio conllu-files
        self.conllu_remote = [i.object_name[14:] for i in list(self.minioClient.list_objects('public', prefix='syntax-parsed/')) if 'short' not in i.object_name]
        # List of local conllu-files
        self.conllu_local = os.listdir(self.DIR_CONLLU)
        # List of local json-files
        self.json_local = os.listdir(self.DIR_JSON)
        # Prepositional government
        with open('prepositional_government.json') as file:
            self.prepositional_government = json.load(file)

    @classmethod
    def get_tokenlists_from_conllu(self, conllu_file_name):
        '''Converts conllu-file content to a list of tokenlists.'''
        file = open(self.DIR_CONLLU+'/'+conllu_file_name, 'r', encoding = 'utf-8')
        data = file.read().split('\n\n')
        tokenlists = []
        for paragraph in data:
            try:
                tokenlists.extend([tokenlist for tokenlist in conllu.parse(paragraph)])
            except:
                pass
        file.close()
        return tokenlists

    def download_from_cosyco(self, conllu_file_name):
        '''Downloads Minio conllu-object.'''
        if conllu_file_name not in self.conllu_local:
            self.minioClient.fget_object('public', 'syntax-parsed/'+conllu_file_name, self.DIR_CONLLU+'/'+conllu_file_name)
            self.conllu_local = os.listdir(self.DIR_CONLLU)
        else:
            print(f'File {conllu_file_name} has already been downloaded. Please check `{self.DIR_CONLLU}` directory.')

    def _count_words(self, tokenlist):
        '''Counts number of words (all tokens except punctuation) in a tokenlist.'''
        return len([token for token in tokenlist if token['upos'] != 'PUNCT'])

    def _extract_verbs(self, tokenlist):
        '''Extracts all verbs (including negative-polarized ones) from a tokenlist.'''
        verbs = []
        for token in tokenlist:
            if token['upos'] == 'VERB':
                verb_lemma = token['lemma'].lower()
                for verb_child in tokenlist:
                    try:
                        if (verb_child['head'] == token['id']) and (verb_child['form'].lower() == 'не'):
                            verb_lemma = 'не_'+verb_lemma
                    except (KeyError, TypeError):
                        pass
                verbs.append(verb_lemma)
        return verbs

    def _extract_nouns(self, tokenlist):
        '''Extracts all nouns with grammar features from a tokenlist.'''
        nouns = []
        case = []
        number = []
        animacy = []
        relation = []
        for token in tokenlist:
            if token['upos'] in ['NOUN', 'PROPN']:
                nouns.append(token['lemma'].lower())
                try:
                    case.append(token['feats']['Case'])
                    number.append(token['feats']['Number'])
                    animacy.append(token['feats']['Animacy'])
                    relation.append(token['deprel'])
                except (KeyError, TypeError):
                    pass
        return nouns, case, number, animacy, relation

    def _extract_prepositions(self, tokenlist):
        '''Extracts all prepositions from a tokenlist.'''
        prepositions = []
        for token in tokenlist:
            if (token['upos'] == 'ADP') and (token['deprel'] == 'case'):
                preposition = token['form'].lower()
                for child in tokenlist:
                    try:
                        if (child['head'] == token['id']) and (child['deprel'] == 'fixed'):
                            preposition += ' '+child['form'].lower()
                    except KeyError:
                        pass
                prepositions.append(preposition)
        return prepositions

    def _extract_combinations(self, tokenlist):
        '''Returns two lists of combinations `verblemma__preposition__nounlemma__nouncase__nounnumber__nounanimacy__noundeprel` from a tokenlist:
            - correct,
            - incorrect (filtered by a prepositional government dictionary)
        '''
        combinations = []
        filtered = []
        for token in tokenlist:
            if token['upos'] == 'VERB':
                verb_lemma = token['lemma']
                for verb_child in tokenlist:
                    try:
                        if (verb_child['head'] == token['id']) and (verb_child['form'].lower() == 'не'):
                            verb_lemma = 'не_'+verb_lemma
                    except (KeyError, TypeError):
                        pass
                for verb_child in tokenlist:
                    try:
                        if (verb_child['head'] == token['id']) and \
                            (verb_child['upos'] in ['NOUN', 'PROPN']):
                            num = 0
                            for noun_child in tokenlist:
                                try:
                                    if (noun_child['head'] == verb_child['id']) and \
                                        (noun_child['upos'] == 'NUM'):
                                        num += 1
                                except (KeyError, TypeError):
                                    pass
                            if num == 0:                    
                                try:
                                    noun_lemma = verb_child['lemma']
                                    noun_case = verb_child['feats']['Case']
                                    noun_number = verb_child['feats']['Number']
                                    noun_anim = verb_child['feats']['Animacy']
                                    noun_rel = verb_child['deprel']
                                    preposition = []
                                    for noun_child in tokenlist:
                                        if (noun_child['head'] == verb_child['id']) and \
                                            (noun_child['upos'] == 'ADP') and \
                                            (noun_child['deprel'] == 'case'):
                                            preposition.append(noun_child['form'].lower())
                                            for adp_child in tokenlist:
                                                if (adp_child['head'] == noun_child['id']) and \
                                                    (adp_child['deprel'] == 'fixed'):
                                                    preposition.append(adp_child['form'].lower())
                                    if preposition:
                                        preposition = ' '.join(preposition)
                                        try:
                                            if (preposition in self.prepositional_government) and \
                                                (noun_case not in self.prepositional_government[preposition]):
                                                filtered.append(str(verb_lemma+'__'+preposition+'__'+noun_lemma+'__'+noun_case+'__'+noun_number+'__'+noun_anim+'__'+noun_rel))
                                            else:
                                                combinations.append(str(verb_lemma+'__'+preposition+'__'+noun_lemma+'__'+noun_case+'__'+noun_number+'__'+noun_anim+'__'+noun_rel))
                                        except:
                                            pass
                                    else:
                                        preposition = 'NO'
                                        combinations.append(str(verb_lemma+'__'+preposition+'__'+noun_lemma+'__'+noun_case+'__'+noun_number+'__'+noun_anim+'__'+noun_rel))
                                except:
                                    pass
                    except (KeyError, TypeError):
                        pass
        return combinations, filtered

    def get_statistics(self, conllu_file_name):
        '''Save all statistics from a given conllu-file to json-file:
            1) count of:
            - sentences,
            - words,
            2) sorted frequency dictionary of:
            - verbs,
            - nouns,
            - nouns' case,
            - nouns' number,
            - nouns' animacy,
            - nouns' relation,
            - prepositions,
            - correct verb combinations,
            - incorrect verb combinations (filtered by a prepositional government dictionary)
        '''
        if conllu_file_name[:-7]+'.json' not in self.json_local:
            if conllu_file_name not in self.conllu_local:
                print(f'Downloading `{conllu_file_name}` from cosyco...')
                self.download_from_cosyco(conllu_file_name)
            print(f'Loading data from `{conllu_file_name}`...')
            tokenlists = self.get_tokenlists_from_conllu(conllu_file_name)
            print(f'Counting statistics from `{conllu_file_name}`...')
            contents_types = ['sentences', 'words', 'verbs', 'nouns', 'case', 'number', 'animacy', 'relation', 'prepositions', 'combinations', 'filtered']
            contents = [0, 0, Counter(), Counter(), Counter(), Counter(), Counter(), Counter(), Counter(), Counter(), Counter()]
            contents[0] = len(tokenlists)
            for tokenlist in tqdm.tqdm(tokenlists):
                contents[1] += self._count_words(tokenlist)
                verbs = self._extract_verbs(tokenlist)
                nouns, case, number, animacy, relation = self._extract_nouns(tokenlist)
                prepositions = self._extract_prepositions(tokenlist)
                for i, j in zip(range(2, 9), [verbs, nouns, case, number, animacy, relation, prepositions]):
                    contents[i].update(j)
                combinations, filtered = self._extract_combinations(tokenlist)
                contents[9].update(combinations)
                contents[10].update(filtered)
            for i in range(2, 11):
                contents[i] = dict(contents[i].most_common())
            to_dump = {}
            for i, j in zip(contents_types, contents):
                to_dump[i] = j
            json_object = json.dumps(to_dump, ensure_ascii=False)
            with open(self.DIR_JSON+'/'+conllu_file_name[:-7]+'.json', 'w', encoding='utf-8') as file:
                file.write(json_object)
            self.json_local = os.listdir(self.DIR_JSON)
        else:
            print(f'For file `{conllu_file_name}` statistics have already been collected. To recollect statistics remove json-file from directory `{self.DIR_JSON}`.')

    def read_statistics(self, json_file_name):
        '''Opens json-file with statistics'''
        with open(self.DIR_JSON+'/'+json_file_name, encoding='utf-8') as file:
            statistics = json.load(file)
        return statistics

    def join_statistics(self, conllu_files: list, save_to=''):
        '''Returns joined statistics of json-files.'''
        for file in conllu_files:
            if file[:-7]+'.json' not in self.json_local:
                raise FileNotFoundError(f'For file `{file}` statistics not found. Apply `get_statistics` function to the file.')
        if save_to:
            if save_to+'.json' in self.json_local:
                raise NameError('File with this name already exists. Please choose another name.')
        stats = {
            'sentences': 0, 
            'words': 0,
            'verbs': Counter(),
            'nouns': Counter(),
            'case': Counter(),
            'number': Counter(),
            'animacy': Counter(),
            'relation': Counter(),
            'prepositions': Counter(),
            'combinations': Counter(),
            'filtered': Counter()
        }
        for file in conllu_files:
            with open(self.DIR_JSON+'/'+file[:-7]+'.json', 'r', encoding='utf-8') as f:
                json_data = json.load(f)
            for i in ['sentences', 'words']:
                stats[i] += json_data[i]
            for i in ['verbs', 'nouns', 'case', 'number', 'animacy', 'relation', 'prepositions', 'combinations', 'filtered']:
                stats[i].update(json_data[i])
        for i in ['verbs', 'nouns', 'case', 'number', 'animacy', 'relation', 'prepositions', 'combinations', 'filtered']:
            stats[i] = dict(stats[i].most_common())
        if save_to:
            json_object = json.dumps(stats, ensure_ascii=False)
            with open(self.DIR_JSON+'/'+save_to+'.json', 'w', encoding='utf-8') as f:
                f.write(json_object)
            self.json_local = os.listdir(self.DIR_JSON)
        else:
            return stats

    def filter_combinations(self, combinations: dict, verb='', prep='', noun='', case='', num='', anim='', rel=''):
        '''Returns filtered frequency dictionary for combinations or combinations filtered by prepositional government.
        Filter can be applied to verb, preposition, noun, case, number of noun or syntax relation tag.'''
        args_dict = {i: j for i, j in enumerate([verb, prep, noun, case, num, anim, rel]) if j}
        args = list(args_dict.items())
        n = len(args)
        if n == 1:
            return (dict(filter(lambda x: \
                x[0].split('__')[args[0][0]] == args[0][1], 
                combinations.items()
            )))
        if n == 2:
            return (dict(filter(lambda x: \
                x[0].split('__')[args[0][0]] == args[0][1] and \
                x[0].split('__')[args[1][0]] == args[1][1],
                combinations.items()
            )))
        if n == 3:
            return (dict(filter(lambda x: \
                x[0].split('__')[args[0][0]] == args[0][1] and \
                x[0].split('__')[args[1][0]] == args[1][1] and \
                x[0].split('__')[args[2][0]] == args[2][1],
                combinations.items()
            )))
        if n == 4:
            return (dict(filter(lambda x: \
                x[0].split('__')[args[0][0]] == args[0][1] and \
                x[0].split('__')[args[1][0]] == args[1][1] and \
                x[0].split('__')[args[2][0]] == args[2][1] and \
                x[0].split('__')[args[3][0]] == args[3][1],
                combinations.items()
            )))
        if n == 5:
            return (dict(filter(lambda x: \
                x[0].split('__')[args[0][0]] == args[0][1] and \
                x[0].split('__')[args[1][0]] == args[1][1] and \
                x[0].split('__')[args[2][0]] == args[2][1] and \
                x[0].split('__')[args[3][0]] == args[3][1] and \
                x[0].split('__')[args[4][0]] == args[4][1],
                combinations.items()
            )))
        if n == 6:
            return (dict(filter(lambda x: \
                x[0].split('__')[args[0][0]] == args[0][1] and \
                x[0].split('__')[args[1][0]] == args[1][1] and \
                x[0].split('__')[args[2][0]] == args[2][1] and \
                x[0].split('__')[args[3][0]] == args[3][1] and \
                x[0].split('__')[args[4][0]] == args[4][1] and \
                x[0].split('__')[args[5][0]] == args[5][1],
                combinations.items()
            )))
        if n == 7:
            return (dict(filter(lambda x: \
                x[0].split('__')[args[0][0]] == args[0][1] and \
                x[0].split('__')[args[1][0]] == args[1][1] and \
                x[0].split('__')[args[2][0]] == args[2][1] and \
                x[0].split('__')[args[3][0]] == args[3][1] and \
                x[0].split('__')[args[4][0]] == args[4][1] and \
                x[0].split('__')[args[5][0]] == args[5][1] and \
                x[0].split('__')[args[6][0]] == args[6][1],
                combinations.items()
            )))
        else:
            return combinations

    def find_text(self, freq_dict: dict, conllu_files: list):
        '''Returns a list of example sentences to a given frequency dictionary of combinations or combinations filtered by prepositional government.'''
        examples = defaultdict(list)
        for key in freq_dict.keys():
            verb, prep, noun, case, num, anim, rel = key.split('__')
            for file in conllu_files:
                print(f'Loading data from {file}...')
                tokenlists = self.get_tokenlists_from_conllu(file)
                for tokenlist in tqdm.tqdm(tokenlists):
                    for token in tokenlist:
                        if token['lemma'].lower() == verb:
                            for verb_child in tokenlist:
                                try:
                                    if (verb_child['head'] == token['id']) and \
                                        (verb_child['upos'] in ['NOUN', 'PROPN']) and \
                                        (verb_child['lemma'].lower() == noun) and \
                                        (verb_child['feats']['Case'] == case) and \
                                        (verb_child['feats']['Number'] == num) and \
                                        (verb_child['feats']['Animacy'] == anim) and \
                                        (verb_child['deprel'] == rel):
                                        num = 0
                                        for noun_child in tokenlist:
                                            if (noun_child['head'] == verb_child['id']) and \
                                                noun_child['upos'] == 'NUM':
                                                num += 1
                                        if num == 0:
                                            if prep != 'NO':
                                                preposition = []
                                                for noun_child in tokenlist:
                                                    if (noun_child['head'] == verb_child['id']) and \
                                                        (noun_child['upos'] == 'ADP') and \
                                                        (noun_child['deprel'] == 'case'):
                                                        preposition.append(noun_child['form'].lower())
                                                        for adp_child in tokenlist:
                                                            if (adp_child['head'] == noun_child['id']) and \
                                                                (adp_child['deprel'] == 'fixed'):
                                                                preposition.append(adp_child['form'].lower())
                                                if ' '.join(preposition) == prep:
                                                    examples[key].append(tokenlist.metadata['text'])
                                            else:
                                                examples[key].append(tokenlist.metadata['text'])
                                except:
                                    pass
        return examples
use sha2::{Sha256, Digest};

fn pronounceable_hash(hash: &String) -> String {
    // Now parentheses from Muhammad Ikhwan Perwira
    // Source https://stackoverflow.com/questions/70912156/how-do-i-produce-spellable-hash-or-pronounceable-hash

    // Can be enhanced by algorithm "The Bubble Babble Binary Data Encoding"
    // Source https://web.mit.edu/kenta/www/one/bubblebabble/spec/jrtrjwzi/draft-huima-01.txt
    // Rust reference https://github.com/reyk/bubblebabble-rs
    const LEN_CONS:usize = 17;
    const LEN_VOW:usize = 5;
    let vowel: [char; LEN_VOW] = ['a','i','u','e','o'];
    let consonant: [char; LEN_CONS] = ['b','c','d','g','h','j','k','l','m','n','p','r','s','t','v','w','y'];

    let mut result: Vec<char> = Vec::new();
    for (i, x) in hash.chars().enumerate() {
      if i%2 == 0 {
        result.push(vowel[(x as usize) % LEN_VOW]);
      } else {
        result.push(consonant[(x as usize) % LEN_CONS]);
      }
    }
    return result.iter().collect();
}

pub fn get_id_from_context(context: &String) -> String { base16ct::lower::encode_string(&Sha256::digest(context.as_bytes())) }

pub fn get_slang_from_id(id: &String) -> String { pronounceable_hash(&(id[..11]).to_owned()) }
/// Checks the qualified CDI name accepted in agent inventories and job selectors.
///
/// Rayflow stores these names in VARCHAR(255) columns. The component grammar
/// follows the CDI qualified name parser.
pub fn is_qualified_cdi_device(device: &str) -> bool {
    if device.len() > 255 {
        return false;
    }
    let Some((kind, name)) = device.split_once('=') else {
        return false;
    };
    let Some((vendor, class)) = kind.split_once('/') else {
        return false;
    };
    valid_component(vendor, false) && valid_component(class, false) && valid_component(name, true)
}

fn valid_component(component: &str, is_device_name: bool) -> bool {
    let bytes = component.as_bytes();
    bytes.first().is_some_and(|first| {
        first.is_ascii_alphabetic() || (is_device_name && first.is_ascii_digit())
    }) && bytes.last().is_some_and(u8::is_ascii_alphanumeric)
        && bytes.iter().all(|&byte| {
            byte.is_ascii_alphanumeric()
                || matches!(byte, b'_' | b'-' | b'.')
                || (is_device_name && byte == b':')
        })
}

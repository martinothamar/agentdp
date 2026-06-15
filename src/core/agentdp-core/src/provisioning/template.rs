pub(in crate::provisioning) fn render(template: &str, replacements: &[(&str, String)]) -> String {
    let mut rendered = template.to_owned();
    for (placeholder, value) in replacements {
        rendered = rendered.replace(placeholder, value);
    }
    rendered
}
